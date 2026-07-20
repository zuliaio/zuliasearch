package io.zulia.server.index.resident;

import io.zulia.server.index.ZuliaIndex;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Pairs a resident index with its lease count, recency, and handle state
 */
class IndexHandle {

	enum HandleState {
		/**
		 * Resident and serving. The only state in which leases can be acquired.
		 */
		ACTIVE,
		/**
		 * Eviction has claimed the handle. No new leases, and it reverts to ACTIVE if a lease raced the mark.
		 */
		DRAINING,
		/**
		 * Terminal. The index is unloaded or deleted and the handle never serves again.
		 */
		CLOSED
	}

	private final ZuliaIndex index;
	private final AtomicInteger leaseCount = new AtomicInteger();
	private final AtomicReference<HandleState> state = new AtomicReference<>(HandleState.ACTIVE);
	private volatile long lastAccessMillis;
	private volatile long evictionBackoffUntilMillis;
	private final long loadedMillis;
	private final CompletableFuture<Void> closed = new CompletableFuture<>();

	IndexHandle(ZuliaIndex index) {
		this.index = index;
		this.loadedMillis = System.currentTimeMillis();
		this.lastAccessMillis = loadedMillis;
	}

	long getLoadedMillis() {
		return loadedMillis;
	}

	long getLastAccessMillis() {
		return lastAccessMillis;
	}

	long getEvictionBackoffUntilMillis() {
		return evictionBackoffUntilMillis;
	}

	void backoffEviction(long untilMillis) {
		this.evictionBackoffUntilMillis = untilMillis;
	}

	ZuliaIndex getIndex() {
		return index;
	}

	IndexLease tryAcquire() {
		return doTryAcquire(true);
	}

	/**
	 * Acquire without refreshing recency, for monitoring reads that must not keep an index resident
	 */
	IndexLease tryAcquireQuietly() {
		return doTryAcquire(false);
	}

	private IndexLease doTryAcquire(boolean updateRecency) {
		leaseCount.incrementAndGet();
		if (!isActive()) {
			leaseCount.decrementAndGet();
			return null;
		}
		if (updateRecency) {
			lastAccessMillis = System.currentTimeMillis();
		}
		return new IndexLease(this);
	}

	void release() {
		leaseCount.decrementAndGet();
	}

	int getLeaseCount() {
		return leaseCount.get();
	}

	void close() {
		state.set(HandleState.CLOSED);
	}

	boolean compareAndSetState(HandleState expectedState, HandleState newState) {
		return state.compareAndSet(expectedState, newState);
	}

	boolean isActive() {
		return state.get() == HandleState.ACTIVE;
	}

	void waitForClosed(int timeout, TimeUnit timeUnit) throws ExecutionException, InterruptedException, TimeoutException {
		closed.get(timeout, timeUnit);
	}

	void markClosedComplete() {
		closed.complete(null);
	}
}