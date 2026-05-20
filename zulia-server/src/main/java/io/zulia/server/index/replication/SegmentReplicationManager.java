package io.zulia.server.index.replication;

import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaIndex.IndexShardMapping;
import io.zulia.message.ZuliaIndex.ShardMapping;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.index.ShardWriteManager;
import io.zulia.server.index.ZuliaShard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SegmentReplicationManager {

	private final static Logger LOG = LoggerFactory.getLogger(SegmentReplicationManager.class);

	private static final int MAX_CONCURRENT_REPLICATIONS = 4;
	private static final int SHUTDOWN_TIMEOUT_SECONDS = 30;
	private static final long CHILD_INTERRUPT_DRAIN_SECONDS = 5;

	private final SegmentPublisher publisher;
	private final IndexShardMapping indexShardMapping;
	private final String indexName;
	private final boolean hasAnyReplicas;
	private final Map<Integer, List<Node>> replicaNodesByShard;

	private final Map<Integer, ShardState> shardStates = new ConcurrentHashMap<>();
	private final Map<ReplicaShardKey, ReplicaState> replicaStates = new ConcurrentHashMap<>();

	private final Semaphore replicationSemaphore = new Semaphore(MAX_CONCURRENT_REPLICATIONS);
	private final ExecutorService replicationExecutor = Executors.newVirtualThreadPerTaskExecutor();
	private volatile boolean shutdown = false;

	public SegmentReplicationManager(InternalClient internalClient, IndexShardMapping indexShardMapping, String indexName, int responseTimeoutMinutes,
			ReplicationRateLimiter rateLimiter) {
		this(new PushSegmentPublisher(internalClient, responseTimeoutMinutes, rateLimiter), indexShardMapping, indexName);
	}

	public SegmentReplicationManager(SegmentPublisher publisher, IndexShardMapping indexShardMapping, String indexName) {
		this.publisher = publisher;
		this.indexShardMapping = indexShardMapping;
		this.indexName = indexName;
		this.replicaNodesByShard = indexShardMapping.getShardMappingList().stream()
				.collect(Collectors.toUnmodifiableMap(ShardMapping::getShardNumber, ShardMapping::getReplicaNodeList));
		this.hasAnyReplicas = replicaNodesByShard.values().stream().anyMatch(l -> !l.isEmpty());
	}

	public void replicateAfterCommit(ZuliaShard primaryShard) {
		if (shutdown || !hasAnyReplicas || getReplicaNodes(primaryShard.getShardNumber()).isEmpty()) {
			return;
		}

		try {
			replicationExecutor.submit(() -> {
				try {
					replicationSemaphore.acquire();
					try {
						doReplicate(primaryShard);
					}
					finally {
						replicationSemaphore.release();
					}
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					LOG.warn("Replication interrupted for {}:s{}", indexName, primaryShard.getShardNumber());
				}
				catch (Exception e) {
					LOG.error("Failed to replicate {}:s{}", indexName, primaryShard.getShardNumber(), e);
				}
			});
		}
		catch (RejectedExecutionException e) {
			// Shutdown race: hook fires on the commit path and must not propagate. Watchdog will retry.
			LOG.debug("Replication rejected for {}:s{} during shutdown", indexName, primaryShard.getShardNumber());
		}
	}

	// Catches replicas that flapped or fell behind during quiet periods with no commit hook.
	public void watchdogTick(Iterable<ZuliaShard> primaryShards) {
		if (shutdown || !hasAnyReplicas) {
			return;
		}
		for (ZuliaShard primary : primaryShards) {
			replicateAfterCommit(primary);
		}
	}

	public void shutdown() {
		shutdown = true;
		replicationExecutor.shutdown();
		try {
			if (!replicationExecutor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
				LOG.warn("Replication tasks for {} did not complete in {} seconds, forcing shutdown", indexName, SHUTDOWN_TIMEOUT_SECONDS);
				replicationExecutor.shutdownNow();
				if (!replicationExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
					LOG.error("Replication tasks for {} did not terminate after forced shutdown", indexName);
				}
			}
		}
		catch (InterruptedException e) {
			replicationExecutor.shutdownNow();
			Thread.currentThread().interrupt();
		}
		try {
			publisher.close();
		}
		catch (Exception e) {
			LOG.warn("Publisher close failed for {}", indexName, e);
		}
		LOG.info("Segment replication manager shut down for {}", indexName);
	}

	private void doReplicate(ZuliaShard primaryShard) throws Exception {
		if (shutdown) {
			return;
		}

		ShardWriteManager writeManager = primaryShard.getShardWriteManager();
		if (writeManager == null) {
			return;
		}

		int shardNumber = primaryShard.getShardNumber();
		List<Node> replicaNodes = getReplicaNodes(shardNumber);
		if (replicaNodes.isEmpty()) {
			return;
		}

		ShardState shardState = shardStates.computeIfAbsent(shardNumber, k -> new ShardState());
		shardState.getLock().lockInterruptibly();
		try (CommitSnapshot snapshot = CommitSnapshot.tryTake(writeManager, indexName, shardNumber)) {
			if (snapshot.isEmpty()) {
				return;
			}

			long generation = snapshot.generation();
			Long lastAttempted = shardState.getLastAttemptedGeneration();
			if (lastAttempted != null && lastAttempted >= generation && allReplicasAtOrAfter(shardNumber, replicaNodes, generation)) {
				return;
			}

			List<Thread> threads = new ArrayList<>(replicaNodes.size());
			for (Node replicaNode : replicaNodes) {
				Thread thread = Thread.ofVirtual().name(indexName + ":s" + shardNumber + "-replicate-" + replicaNode.getServerAddress() + ":"
						+ replicaNode.getServicePort()).start(() -> replicateToReplica(shardNumber, replicaNode, snapshot, generation));
				threads.add(thread);
			}
			joinAll(threads);

			// Advance unconditionally; allReplicasAtOrAfter at the top of the method forces retry for laggards.
			shardState.setLastAttemptedGeneration(generation);
		}
		finally {
			shardState.getLock().unlock();
		}
	}

	// Taxonomy publishes before index: new-taxo/old-index is safe, new-index/old-taxo fails ordinal lookup.
	private void replicateToReplica(int shardNumber, Node replicaNode, CommitSnapshot snapshot, long generation) {
		ReplicaShardKey key = ReplicaShardKey.of(replicaNode, shardNumber);
		ReplicaState state = replicaStates.computeIfAbsent(key, k -> new ReplicaState());
		if (state.isCircuitOpen()) {
			LOG.debug("Circuit open for {}:s{} to {}:{}, skipping replication", indexName, shardNumber, replicaNode.getServerAddress(),
					replicaNode.getServicePort());
			return;
		}
		try {
			if (snapshot.taxoDirectory() != null) {
				publisher.publish(new PublishRequest(indexName, shardNumber, replicaNode, snapshot.taxoDirectory(), snapshot.taxoFiles(), generation, true));
			}
			publisher.publish(new PublishRequest(indexName, shardNumber, replicaNode, snapshot.indexDirectory(), snapshot.indexFiles(), generation, false));
			state.recordSuccess(generation);
		}
		catch (Exception e) {
			ReplicaState.FailureOutcome outcome = state.recordFailure();
			if (outcome.openedBackoffMs() > 0) {
				LOG.warn("Circuit opened for {}: {} consecutive failures, backing off {}ms", key, outcome.consecutiveFailures(), outcome.openedBackoffMs());
			}
			LOG.warn("Failed to replicate {}:s{} to {}:{}: {}", indexName, shardNumber, replicaNode.getServerAddress(), replicaNode.getServicePort(),
					e.getMessage());
		}
	}

	private boolean allReplicasAtOrAfter(int shardNumber, List<Node> replicaNodes, long generation) {
		for (Node node : replicaNodes) {
			ReplicaState state = replicaStates.get(ReplicaShardKey.of(node, shardNumber));
			Long replicaGen = (state == null) ? null : state.getGeneration();
			if (replicaGen == null || replicaGen < generation) {
				return false;
			}
		}
		return true;
	}

	// On interrupt, drain children before returning; the finally above releases the IndexCommit they read from.
	private void joinAll(List<Thread> threads) {
		try {
			for (Thread thread : threads) {
				thread.join();
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			for (Thread t : threads) {
				t.interrupt();
			}
			long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(CHILD_INTERRUPT_DRAIN_SECONDS);
			for (Thread t : threads) {
				long remainingNanos = deadlineNanos - System.nanoTime();
				if (remainingNanos <= 0) {
					break;
				}
				try {
					t.join(TimeUnit.NANOSECONDS.toMillis(remainingNanos));
				}
				catch (InterruptedException ignored) {
					break;
				}
			}
		}
	}

	private List<Node> getReplicaNodes(int shardNumber) {
		return replicaNodesByShard.getOrDefault(shardNumber, List.of());
	}

	public List<ReplicationShardState> getReplicationState() {
		List<ReplicationShardState> result = new ArrayList<>(replicaNodesByShard.size());
		long now = System.currentTimeMillis();
		for (ShardMapping shardMapping : indexShardMapping.getShardMappingList()) {
			int shardNumber = shardMapping.getShardNumber();
			ShardState shardState = shardStates.get(shardNumber);
			Long primaryGen = (shardState == null) ? null : shardState.getLastAttemptedGeneration();
			List<ReplicaReplicationState> replicaSnapshots = new ArrayList<>(shardMapping.getReplicaNodeList().size());
			for (Node replicaNode : shardMapping.getReplicaNodeList()) {
				ReplicaState state = replicaStates.get(ReplicaShardKey.of(replicaNode, shardNumber));
				ReplicaState.Snapshot snap = (state == null) ? ReplicaState.EMPTY_SNAPSHOT : state.snapshot(now);
				Long lagGenerations = (primaryGen != null && snap.generation() != null) ? primaryGen - snap.generation() : null;
				Long msSinceLastSuccess = (snap.lastSuccessMs() != null) ? now - snap.lastSuccessMs() : null;
				replicaSnapshots.add(new ReplicaReplicationState(replicaNode.getServerAddress(), replicaNode.getServicePort(), snap.generation(), lagGenerations,
						snap.lastSuccessMs(), msSinceLastSuccess, snap.consecutiveFailures(), snap.circuitOpen(), snap.backoffUntilMs()));
			}
			result.add(new ReplicationShardState(shardNumber, primaryGen, Collections.unmodifiableList(replicaSnapshots)));
		}
		return Collections.unmodifiableList(result);
	}
}
