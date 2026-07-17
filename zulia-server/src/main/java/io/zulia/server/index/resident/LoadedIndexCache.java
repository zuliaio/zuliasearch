package io.zulia.server.index.resident;

import io.zulia.message.ZuliaIndex.IndexShardMapping;
import io.zulia.message.ZuliaIndex.ShardMapping;
import io.zulia.server.exceptions.IndexDoesNotExistException;
import io.zulia.server.index.ZuliaIndex;
import io.zulia.server.index.resident.IndexHandle.HandleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Owns the registry of every index defined for this node and the set of indexes resident in memory.
 * Non-transient indexes (the default) load at startup and stay resident. Indexes marked transient in
 * their settings load lazily on first access, and when the node's {@link TransientIndexPolicy} sets a
 * bound, an evictor unloads the longest-idle transient index back to disk. Callers only hold a resident
 * index through an {@link IndexLease}, so eviction is deferred while any operation is in flight.
 */
public class LoadedIndexCache {

	private final static Logger LOG = LoggerFactory.getLogger(LoadedIndexCache.class);

	// How often the evictor re-examines resident transient indexes.
	private static final long EVICTOR_INTERVAL_MS = 5_000;

	// A just-loaded index cannot be evicted until this old, so size pressure never unloads it before first use
	private static final long MIN_RESIDENCY_MILLIS = 10_000;

	// Bound for waiting on a draining handle to finish closing before a fresh load of the same index.
	private static final long DRAIN_WAIT_MILLIS = 120_000;

	// Bounds the IO of a wildcard query faulting in many transient indexes while matching the
	// parallelism startup loads have always had.
	private static final int LOAD_PERMITS = Math.max(2, Runtime.getRuntime().availableProcessors());

	private final ConcurrentHashMap<String, RegisteredIndex> registeredIndexMap = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<String, IndexHandle> residentIndexMap = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<String, Lock> indexLockMap = new ConcurrentHashMap<>();
	private final Semaphore loadSemaphore = new Semaphore(LOAD_PERMITS);
	private final Semaphore evictorSignal = new Semaphore(0);
	private final LongAdder loadCount = new LongAdder();
	private final LongAdder evictionCount = new LongAdder();

	private final TransientIndexPolicy policy;
	private final IndexLoader loader;
	private final Thread evictorThread;
	private volatile boolean running = true;

	public LoadedIndexCache(TransientIndexPolicy policy, IndexLoader loader) {
		this.policy = policy;
		this.loader = loader;
		this.evictorThread = policy.enabled() ? Thread.ofVirtual().name("index-evictor").start(this::evictorLoop) : null;
	}

	public void register(RegisteredIndex registeredIndex) {
		registeredIndexMap.put(registeredIndex.indexName(), registeredIndex);
	}

	public boolean isRegistered(String indexName) {
		return registeredIndexMap.containsKey(indexName);
	}

	public RegisteredIndex getRegistered(String indexName) {
		return registeredIndexMap.get(indexName);
	}

	public Set<String> getRegisteredIndexNames() {
		return Collections.unmodifiableSet(registeredIndexMap.keySet());
	}

	public Collection<RegisteredIndex> getRegisteredIndexes() {
		return Collections.unmodifiableCollection(registeredIndexMap.values());
	}

	/**
	 * Leases the resident index without loading, or returns null when it is not resident.
	 */
	public IndexLease tryLease(String indexName) {
		IndexHandle handle = residentIndexMap.get(indexName);
		return handle != null ? handle.tryAcquire() : null;
	}

	/**
	 * Leases the resident index without loading or refreshing recency, or returns null when it is not resident.
	 */
	public IndexLease tryLeaseQuietly(String indexName) {
		IndexHandle handle = residentIndexMap.get(indexName);
		return handle != null ? handle.tryAcquireQuietly() : null;
	}

	/**
	 * Leases the resident index without loading, waiting out a draining handle first. A single failed
	 * acquire does not mean the index is gone: the evictor can observe this very lease attempt and
	 * revert its DRAINING mark to ACTIVE. Returns null only when the index is genuinely not resident,
	 * meaning it was never loaded or the drain completed. May wait like leaseOrLoad's drain path does.
	 */
	public IndexLease tryLeaseResident(String indexName) throws Exception {
		IndexHandle handle = residentIndexMap.get(indexName);
		if (handle == null) {
			return null;
		}
		IndexLease lease = handle.tryAcquire();
		if (lease != null) {
			return lease;
		}
		return awaitDrainOutcome(handle);
	}

	/**
	 * Leases the resident index, loading it first when it is registered but not resident. Returns null when
	 * the index is not registered.
	 */
	public IndexLease leaseOrLoad(String indexName) throws Exception {
		IndexLease lease = tryLease(indexName);
		if (lease != null) {
			return lease;
		}
		if (!registeredIndexMap.containsKey(indexName)) {
			return null;
		}
		Lock lock = getUpdateLock(indexName);
		lock.lock();
		try {
			lease = tryLease(indexName);
			if (lease != null) {
				return lease;
			}
			IndexHandle draining = residentIndexMap.get(indexName);
			if (draining != null) {
				lease = awaitDrainOutcome(draining);
				if (lease != null) {
					return lease;
				}
			}
			if (!registeredIndexMap.containsKey(indexName)) {
				return null;
			}
			return loadLocked(indexName);
		}
		finally {
			lock.unlock();
		}
	}

	// A DRAINING handle either finishes closing or reverts to ACTIVE when a lease raced the evictor's mark
	private IndexLease awaitDrainOutcome(IndexHandle handle) throws Exception {
		long deadline = System.nanoTime() + DRAIN_WAIT_MILLIS * 1_000_000L;
		while (System.nanoTime() < deadline) {
			IndexLease lease = handle.tryAcquire();
			if (lease != null) {
				return lease;
			}
			try {
				handle.waitForClosed(100, TimeUnit.MILLISECONDS);
				return null;
			}
			catch (TimeoutException stillDraining) {
			}
		}
		throw new IOException("Timed out waiting for index <" + handle.getIndex().getIndexName() + "> to finish unloading after " + DRAIN_WAIT_MILLIS + "ms");
	}

	private IndexLease loadLocked(String indexName) throws Exception {
		loadSemaphore.acquire();
		try {
			long start = System.currentTimeMillis();
			ZuliaIndex index;
			try {
				index = loader.load(indexName);
			}
			catch (IndexDoesNotExistException e) {
				// the index service no longer knows this index (i.e., a partially failed delete)
				// heal the registry, so wildcard expansion and routing stop advertising it
				registeredIndexMap.remove(indexName);
				throw e;
			}

			IndexHandle handle = new IndexHandle(index);
			IndexLease lease = handle.tryAcquire();
			residentIndexMap.put(indexName, handle);
			registeredIndexMap.put(indexName, new RegisteredIndex(indexName, index.getIndexShardMapping()));
			loadCount.increment();
			LOG.info("Loaded index {} in {}ms ({} resident)", indexName, System.currentTimeMillis() - start, residentIndexMap.size());
			evictorSignal.release();
			return lease;
		}
		finally {
			loadSemaphore.release();
		}
	}

	/**
	 * Per-index lock serializing loads, unload waits, deletes, and settings updates for one index name.
	 * Must not be held while federating to other nodes: the federated call comes back to this node on
	 * another thread and may need this same lock.
	 */
	public Lock getUpdateLock(String indexName) {
		return indexLockMap.computeIfAbsent(indexName, name -> new ReentrantLock());
	}

	/**
	 * Forgets a deleted index entirely. The caller has already deleted the index's data under the update lock.
	 */
	public void removeDeleted(String indexName) {
		registeredIndexMap.remove(indexName);
		IndexHandle handle = residentIndexMap.get(indexName);
		if (handle != null) {
			// close before removing from the map so a thread that already fetched the handle cannot
			// acquire a fresh lease on the deleted index
			handle.close();
			residentIndexMap.remove(indexName, handle);
			handle.markClosedComplete();
		}
	}

	public List<String> getResidentIndexNames() {
		return List.copyOf(residentIndexMap.keySet());
	}

	public int getResidentCount() {
		return residentIndexMap.size();
	}

	public long getLoadCount() {
		return loadCount.sum();
	}

	public long getEvictionCount() {
		return evictionCount.sum();
	}

	private void evictorLoop() {
		while (running) {
			try {
				if (evictorSignal.tryAcquire(EVICTOR_INTERVAL_MS, TimeUnit.MILLISECONDS)) {
					evictorSignal.drainPermits();
				}
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
			if (!running) {
				return;
			}
			try {
				evictionPass();
			}
			catch (Exception e) {
				LOG.error("Eviction pass failed", e);
			}
		}
	}

	// snapshot of a candidate's recency so the eviction sort key is immutable while sorting
	private record EvictionCandidate(String indexName, IndexHandle handle, long lastAccessMillis) {

	}

	private void evictionPass() {
		long now = System.currentTimeMillis();

		List<EvictionCandidate> transientResident = residentIndexMap.entrySet().stream()
				.filter(entry -> entry.getValue().isActive() && isTransient(entry.getValue()))
				.map(entry -> new EvictionCandidate(entry.getKey(), entry.getValue(), entry.getValue().getLastAccessMillis()))
				.sorted(Comparator.comparingLong(EvictionCandidate::lastAccessMillis)).toList();

		int overCount = policy.maxLoadedIndexes() > 0 ? transientResident.size() - policy.maxLoadedIndexes() : 0;
		long idleCutoff = policy.idleTimeoutSeconds() > 0 ? now - policy.idleTimeoutSeconds() * 1000L : Long.MIN_VALUE;

		for (EvictionCandidate candidate : transientResident) {
			IndexHandle handle = candidate.handle();
			boolean idleExpired = handle.getLastAccessMillis() < idleCutoff;
			if (overCount <= 0 && !idleExpired) {
				continue;
			}
			if (!isEvictable(handle, now)) {
				continue;
			}
			if (beginDrain(handle)) {
				overCount--;
				unloadDraining(candidate.indexName(), handle, now);
			}
		}
	}

	private static boolean isTransient(IndexHandle handle) {
		return handle.getIndex().getIndexConfig().getIndexSettings().getTransientIndex();
	}

	private boolean isEvictable(IndexHandle handle, long now) {
		if (now - handle.getLoadedMillis() < MIN_RESIDENCY_MILLIS) {
			return false;
		}
		if (handle.getLeaseCount() > 0) {
			return false;
		}
		ZuliaIndex index = handle.getIndex();
		if (!policy.evictReplicated() && hasReplicas(index.getIndexShardMapping())) {
			return false;
		}
		// dirty primaries commit within the index's commit interval, so wait rather than force a commit-and-close cycle
		return !index.isDirty();
	}

	private static boolean hasReplicas(IndexShardMapping mapping) {
		for (ShardMapping shardMapping : mapping.getShardMappingList()) {
			if (shardMapping.getReplicaNodeCount() > 0) {
				return true;
			}
		}
		return false;
	}

	// Mark first, then re-check the lease count: pairs with doTryAcquire's increment-then-check so a racing
	// lease either forces the revert here or observes DRAINING and backs off. Both transitions are CAS so a
	// concurrent delete that closed the handle is never resurrected into DRAINING or ACTIVE.
	private boolean beginDrain(IndexHandle handle) {
		if (!handle.compareAndSetState(HandleState.ACTIVE, HandleState.DRAINING)) {
			return false;
		}
		if (handle.getLeaseCount() > 0) {
			handle.compareAndSetState(HandleState.DRAINING, HandleState.ACTIVE);
			return false;
		}
		return true;
	}

	private void unloadDraining(String indexName, IndexHandle handle, long now) {
		long idleSeconds = (now - handle.getLastAccessMillis()) / 1000;
		try {
			startUnloadThread(indexName, handle, idleSeconds);
		}
		catch (Throwable t) {
			// the unload thread never started, so revert or the handle is stranded DRAINING forever
			handle.compareAndSetState(HandleState.DRAINING, HandleState.ACTIVE);
			LOG.error("Failed to start eviction of index {}", indexName, t);
		}
	}

	private void startUnloadThread(String indexName, IndexHandle handle, long idleSeconds) {
		Thread.ofVirtual().name("evict-" + indexName).start(() -> {
			long start = System.currentTimeMillis();
			try {
				handle.getIndex().unload(false);
				evictionCount.increment();
				LOG.info("Evicted index {} after {}s idle (unload took {}ms)", indexName, idleSeconds, System.currentTimeMillis() - start);
			}
			catch (Exception e) {
				LOG.error("Failed to unload index {} during eviction", indexName, e);
			}
			finally {
				handle.close();
				residentIndexMap.remove(indexName, handle);
				handle.markClosedComplete();
			}
		});
	}

	/**
	 * Stops the evictor and unloads every resident index, waiting out any eviction already in flight.
	 */
	public void shutdown() {
		running = false;
		if (evictorThread != null) {
			evictorSignal.release();
			try {
				evictorThread.join(5_000);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

		residentIndexMap.values().parallelStream().forEach(handle -> {
			// winning the CAS gives this thread sole ownership of the unload, so a still-running evictor
			// pass or a second shutdown call can never unload the same handle twice
			if (handle.compareAndSetState(HandleState.ACTIVE, HandleState.CLOSED)) {
				try {
					handle.getIndex().unload(false);
				}
				catch (IOException e) {
					LOG.error("Failed to unload index: {}", handle.getIndex().getIndexName(), e);
				}
				finally {
					handle.markClosedComplete();
				}
			}
			else {
				try {
					handle.waitForClosed(30, TimeUnit.SECONDS);
				}
				catch (Exception e) {
					LOG.warn("Index {} did not finish unloading during shutdown", handle.getIndex().getIndexName());
				}
			}
		});
	}
}
