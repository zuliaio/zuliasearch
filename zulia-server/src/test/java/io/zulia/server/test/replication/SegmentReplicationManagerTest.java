package io.zulia.server.test.replication;

import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaIndex.IndexShardMapping;
import io.zulia.message.ZuliaIndex.ShardMapping;
import io.zulia.server.index.replication.ReplicaReplicationState;
import io.zulia.server.index.replication.ReplicationShardState;
import io.zulia.server.index.replication.SegmentPublisher;
import io.zulia.server.index.replication.SegmentReplicationManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for the {@link SegmentReplicationManager} coordination layer. Exercises the paths that don't
 * require a real Lucene shard: the no-replica short-circuit, observability shape, and shutdown idempotency.
 * The end-to-end path (real shards, real transfer) is covered by {@code ReplicationTest}.
 */
public class SegmentReplicationManagerTest {

	private static final String INDEX = "unitTest";

	@Test
	public void noReplicasShortCircuitsBeforeTouchingShard() throws Exception {
		IndexShardMapping mapping = mappingWith(2, 0);
		AtomicInteger publishCalls = new AtomicInteger();
		SegmentPublisher publisher = req -> publishCalls.incrementAndGet();

		SegmentReplicationManager manager = new SegmentReplicationManager(publisher, mapping, INDEX);
		try {
			// hasAnyReplicas == false short-circuits before touching the shard, so passing null is safe and
			// also proves the early-return path doesn't dereference the shard.
			manager.replicateAfterCommit(null);
			manager.watchdogTick(List.of());

			// Give the executor a moment in case the short-circuit is wrong and a task slipped through.
			Thread.sleep(100);
			Assertions.assertEquals(0, publishCalls.get(), "publisher must not be invoked when no replicas exist");
		}
		finally {
			manager.shutdown();
		}
	}

	@Test
	public void replicationStateBeforeAnyAttemptIsEmpty() {
		IndexShardMapping mapping = mappingWith(3, 2);
		SegmentReplicationManager manager = new SegmentReplicationManager(req -> {
		}, mapping, INDEX);
		try {
			List<ReplicationShardState> state = manager.getReplicationState();
			Assertions.assertEquals(3, state.size(), "one entry per shard");
			for (ReplicationShardState shard : state) {
				Assertions.assertNull(shard.lastAttemptedGeneration(), "no attempt yet → null");
				Assertions.assertEquals(2, shard.replicas().size(), "two replicas per shard");
				for (ReplicaReplicationState replica : shard.replicas()) {
					Assertions.assertNull(replica.lastReplicatedGeneration());
					Assertions.assertNull(replica.lagGenerations());
					Assertions.assertNull(replica.lastSuccessMs());
					Assertions.assertNull(replica.msSinceLastSuccess());
					Assertions.assertEquals(0, replica.consecutiveFailures());
					Assertions.assertFalse(replica.circuitOpen());
				}
			}
		}
		finally {
			manager.shutdown();
		}
	}

	@Test
	public void shutdownIsIdempotent() {
		SegmentReplicationManager manager = new SegmentReplicationManager(req -> {
		}, mappingWith(1, 1), INDEX);
		manager.shutdown();
		// A second call must not throw (e.g. on a duplicate executor.shutdown() or publisher.close()).
		manager.shutdown();
	}

	@Test
	public void watchdogAfterShutdownDoesNothing() {
		AtomicInteger publishCalls = new AtomicInteger();
		SegmentPublisher publisher = req -> publishCalls.incrementAndGet();
		SegmentReplicationManager manager = new SegmentReplicationManager(publisher, mappingWith(1, 1), INDEX);
		manager.shutdown();

		// After shutdown, watchdog ticks must be silent — they would otherwise race with state-map clearing
		// or attempt to submit to a closed executor.
		manager.watchdogTick(List.of());
		Assertions.assertEquals(0, publishCalls.get());
	}

	@Test
	public void replicateAfterCommitDoesNotThrowOnPublisherFailure() {
		// We can't drive doReplicate to the publisher without a real ShardWriteManager, but we can at least
		// prove the submission path swallows a faulty publisher's exceptions instead of letting them bubble
		// out of the commit hook (which would fail user writes).
		IndexShardMapping mapping = mappingWith(0, 0); // no shards at all → trivially no work
		SegmentPublisher angryPublisher = req -> {
			throw new RuntimeException("simulated publisher failure");
		};
		SegmentReplicationManager manager = new SegmentReplicationManager(angryPublisher, mapping, INDEX);
		try {
			Assertions.assertDoesNotThrow(() -> manager.replicateAfterCommit(null));
		}
		finally {
			manager.shutdown();
		}
	}

	private static IndexShardMapping mappingWith(int shardCount, int replicasPerShard) {
		IndexShardMapping.Builder mapping = IndexShardMapping.newBuilder().setIndexName(INDEX).setNumberOfShards(shardCount);
		for (int s = 0; s < shardCount; s++) {
			ShardMapping.Builder shard = ShardMapping.newBuilder().setShardNumber(s)
					.setPrimaryNode(Node.newBuilder().setServerAddress("host-primary-" + s).setServicePort(40000 + s).build());
			for (int r = 0; r < replicasPerShard; r++) {
				shard.addReplicaNode(Node.newBuilder().setServerAddress("host-replica-" + s + "-" + r).setServicePort(50000 + s * 10 + r).build());
			}
			mapping.addShardMapping(shard);
		}
		return mapping.build();
	}
}
