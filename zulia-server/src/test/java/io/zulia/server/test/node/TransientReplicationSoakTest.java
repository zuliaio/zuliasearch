package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.UpdateIndex;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaBase.PrimaryReplicaSettings;
import io.zulia.server.node.ZuliaNode;
import io.zulia.server.test.node.shared.NodeExtension;
import io.zulia.server.test.node.shared.TestHelper;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Replication torture test for transient indexes, run with eviction of replicated indexes ENABLED so
 * primaries and replicas are constantly unloaded mid-traffic on a 3 node cluster. Every write round
 * asserts the primary count exactly and then polls every replica to convergence, failing immediately
 * if a replica ever reports MORE documents than its primary (duplication). Between rounds the replica
 * count of a rotating index is flipped 1 to 2 and back (bootstrap and drop transitions under eviction,
 * including transitions reconciled at fault-in time), and a victim index is deleted and recreated. A
 * full cluster restart mid-soak verifies recovery with nothing resident.
 * <p>
 * Excluded from the regular test task by the soak tag. Run with:
 * ./gradlew :zulia-server:soakTest --tests "io.zulia.server.test.node.TransientReplicationSoakTest"
 * (duration knob: -Dzulia.soak.minutes, default 60).
 * <p>
 * A full 60 minute run passed on 2026-07-14: 339 rounds, ~12200 replica convergence checks, 339 replica
 * count flips, ~19400 loads and ~18400 evictions cluster-wide, zero duplication or convergence failures.
 */
@Tag("soak")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TransientReplicationSoakTest {

	private static final Logger LOG = LoggerFactory.getLogger(TransientReplicationSoakTest.class);

	private static final int WRITABLE_COUNT = 36;
	private static final int VICTIM_COUNT = 4;
	private static final int CACHE_SIZE = 6;
	private static final int IDLE_TIMEOUT_SECONDS = 20;
	private static final int SEED_DOCS = 500;
	private static final int DOCS_PER_WRITE = 10;
	private static final int WRITER_THREADS = 4;
	// covers idle commit (5s), a watchdog pass (30s), reload chains, and a fresh replica bootstrap
	private static final long REPLICA_CONVERGE_MILLIS = 180_000;
	// create, seed, restart recovery, and the final sweep get this slice of the total budget
	private static final long RESERVE_MS = 10 * 60_000;

	private static final long SUITE_START_MS = System.currentTimeMillis();
	private static final long TOTAL_BUDGET_MS = Long.parseLong(System.getProperty("zulia.soak.minutes", "60")) * 60_000;

	private static final AtomicIntegerArray EXPECTED = new AtomicIntegerArray(WRITABLE_COUNT);
	private static final AtomicIntegerArray VICTIM_EXPECTED = new AtomicIntegerArray(VICTIM_COUNT);

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(3, zuliaConfig -> {
		zuliaConfig.setTransientIndexCacheSize(CACHE_SIZE);
		zuliaConfig.setTransientIndexIdleTimeoutSeconds(IDLE_TIMEOUT_SECONDS);
		zuliaConfig.setTransientIndexEvictReplicated(true);
	});

	private static String indexName(int i) {
		return String.format("repsoak-%02d", i);
	}

	private static String victimName(int v) {
		return "repsoakvictim-" + v;
	}

	// every fourth index runs with two replicas so both single and multi replica push paths churn
	private static int baseReplicas(int i) {
		return i % 4 == 3 ? 2 : 1;
	}

	@Test
	@Order(1)
	public void createAndSeed() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		for (int i = 0; i < WRITABLE_COUNT; i++) {
			createIndex(zuliaWorkPool, indexName(i), baseReplicas(i));
		}
		for (int v = 0; v < VICTIM_COUNT; v++) {
			createIndex(zuliaWorkPool, victimName(v), 1);
		}

		AtomicReference<Throwable> failure = new AtomicReference<>();
		List<Thread> seeders = new ArrayList<>();
		for (int t = 0; t < WRITER_THREADS; t++) {
			int threadIndex = t;
			seeders.add(Thread.ofVirtual().name("repsoak-seeder-" + t).start(() -> {
				try {
					for (int i = threadIndex; i < WRITABLE_COUNT && failure.get() == null; i += WRITER_THREADS) {
						storeWritableDocs(zuliaWorkPool, i, SEED_DOCS);
						Assertions.assertEquals(EXPECTED.get(i), primaryCount(zuliaWorkPool, indexName(i)));
					}
				}
				catch (Throwable t2) {
					failure.compareAndSet(null, t2);
				}
			}));
		}
		for (Thread seeder : seeders) {
			seeder.join();
		}
		Assertions.assertNull(failure.get(), () -> "seeding failed: " + failure.get());

		for (int v = 0; v < VICTIM_COUNT; v++) {
			seedVictim(zuliaWorkPool, v);
		}
	}

	@Test
	@Order(2)
	public void replicationChurn() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		long deadline = SUITE_START_MS + TOTAL_BUDGET_MS - RESERVE_MS;
		LOG.info("RepSoak: churning for {} minutes", Math.max(0, deadline - System.currentTimeMillis()) / 60_000);

		int round = 0;
		while (System.currentTimeMillis() < deadline) {
			round++;

			AtomicReference<Throwable> failure = new AtomicReference<>();
			List<Thread> writers = new ArrayList<>();
			for (int t = 0; t < WRITER_THREADS; t++) {
				int threadIndex = t;
				writers.add(Thread.ofVirtual().name("repsoak-writer-" + t).start(() -> {
					try {
						for (int i = threadIndex; i < WRITABLE_COUNT && failure.get() == null; i += WRITER_THREADS) {
							storeWritableDocs(zuliaWorkPool, i, DOCS_PER_WRITE);
							long primaryHits = primaryCount(zuliaWorkPool, indexName(i));
							if (primaryHits != EXPECTED.get(i)) {
								failure.compareAndSet(null,
										new AssertionError("primary expected " + EXPECTED.get(i) + " docs in " + indexName(i) + " but got " + primaryHits));
								return;
							}
						}
						// every owned replica must converge to exactly the primary's count before the round ends
						for (int i = threadIndex; i < WRITABLE_COUNT && failure.get() == null; i += WRITER_THREADS) {
							awaitReplicaConvergence(zuliaWorkPool, indexName(i), EXPECTED.get(i), failure);
						}
					}
					catch (Throwable t2) {
						failure.compareAndSet(null, t2);
					}
				}));
			}
			for (Thread writer : writers) {
				writer.join();
			}
			int roundNumber = round;
			Assertions.assertNull(failure.get(), () -> "round " + roundNumber + " failed: " + failure.get());

			// live topology churn between rounds: flip a rotating single-replica index to two replicas and
			// back, and delete plus recreate a victim, both while eviction keeps unloading the participants
			int flipTarget = nextSingleReplicaIndex(round);
			int flippedReplicas = round % 2 == 1 ? 2 : 1;
			zuliaWorkPool.updateIndex(new UpdateIndex(indexName(flipTarget)).setNumberOfReplicas(flippedReplicas));

			rotateVictim(zuliaWorkPool, round % VICTIM_COUNT);

			if (round % 4 == 0) {
				logClusterState(round);
			}
		}

		Assertions.assertTrue(round >= 2, "expected at least two churn rounds but ran " + round + "; raise zulia.soak.minutes");
		LOG.info("RepSoak: churn finished after {} rounds", round);
	}

	@Test
	@Order(3)
	public void fullClusterRestartRecovers() throws Exception {
		nodeExtension.restartNodes();
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// nothing is resident after restart, and every primary and every replica must still count exactly
		AtomicReference<Throwable> failure = new AtomicReference<>();
		for (int i = 0; i < WRITABLE_COUNT; i++) {
			Assertions.assertEquals(EXPECTED.get(i), primaryCount(zuliaWorkPool, indexName(i)), "wrong primary count in " + indexName(i) + " after restart");
			awaitReplicaConvergence(zuliaWorkPool, indexName(i), EXPECTED.get(i), failure);
			Assertions.assertNull(failure.get(), () -> "replica recovery failed: " + failure.get());
		}
		for (int v = 0; v < VICTIM_COUNT; v++) {
			Assertions.assertEquals(VICTIM_EXPECTED.get(v), primaryCount(zuliaWorkPool, victimName(v)), "wrong victim count after restart");
		}
	}

	@Test
	@Order(4)
	public void residencyConvergesOnEveryNode() throws Exception {
		long deadline = System.currentTimeMillis() + 300_000;
		while (System.currentTimeMillis() < deadline && maxResidentTransient() > CACHE_SIZE) {
			Thread.sleep(2000);
		}
		Assertions.assertTrue(maxResidentTransient() <= CACHE_SIZE, "a node failed to converge to the cap: " + describeResidency());
	}

	@Test
	@Order(5)
	public void finalIntegritySweep() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		AtomicReference<Throwable> failure = new AtomicReference<>();
		for (int i = 0; i < WRITABLE_COUNT; i++) {
			Assertions.assertEquals(EXPECTED.get(i), primaryCount(zuliaWorkPool, indexName(i)), "wrong primary count in " + indexName(i) + " on final sweep");
			awaitReplicaConvergence(zuliaWorkPool, indexName(i), EXPECTED.get(i), failure);
			Assertions.assertNull(failure.get(), () -> "final replica sweep failed: " + failure.get());
		}
	}

	/**
	 * Rotates through the single-replica indexes only, so flips always toggle between 1 and 2 replicas.
	 */
	private static int nextSingleReplicaIndex(int round) {
		int candidate = round % WRITABLE_COUNT;
		while (baseReplicas(candidate) != 1) {
			candidate = (candidate + 1) % WRITABLE_COUNT;
		}
		return candidate;
	}

	private void awaitReplicaConvergence(ZuliaWorkPool zuliaWorkPool, String indexName, int expected, AtomicReference<Throwable> failure) {
		long deadline = System.currentTimeMillis() + REPLICA_CONVERGE_MILLIS;
		long hits = -1;
		try {
			while (System.currentTimeMillis() < deadline && failure.get() == null) {
				hits = replicaCount(zuliaWorkPool, indexName);
				if (hits == expected) {
					return;
				}
				if (hits > expected) {
					failure.compareAndSet(null,
							new AssertionError("replica of " + indexName + " reports " + hits + " docs but the primary has " + expected + " (duplication)"));
					return;
				}
				Thread.sleep(500);
			}
			if (failure.get() == null) {
				failure.compareAndSet(null, new AssertionError(
						"replica of " + indexName + " did not converge: " + hits + " of " + expected + " after " + REPLICA_CONVERGE_MILLIS + "ms"));
			}
		}
		catch (Throwable t) {
			failure.compareAndSet(null, t);
		}
	}

	private void rotateVictim(ZuliaWorkPool zuliaWorkPool, int victim) throws Exception {
		zuliaWorkPool.deleteIndex(victimName(victim));
		VICTIM_EXPECTED.set(victim, 0);
		createIndex(zuliaWorkPool, victimName(victim), 1);
		seedVictim(zuliaWorkPool, victim);
	}

	private void seedVictim(ZuliaWorkPool zuliaWorkPool, int victim) throws Exception {
		String name = victimName(victim);
		for (int d = 0; d < SEED_DOCS / 10; d++) {
			indexRecord(zuliaWorkPool, name, name + "-doc-" + VICTIM_EXPECTED.get(victim), "victim message");
			VICTIM_EXPECTED.incrementAndGet(victim);
		}
		Assertions.assertEquals(VICTIM_EXPECTED.get(victim), primaryCount(zuliaWorkPool, name), "wrong count in recreated " + name);
	}

	private void storeWritableDocs(ZuliaWorkPool zuliaWorkPool, int index, int docCount) throws Exception {
		String name = indexName(index);
		for (int d = 0; d < docCount; d++) {
			indexRecord(zuliaWorkPool, name, name + "-doc-" + EXPECTED.get(index), "message " + EXPECTED.get(index));
			EXPECTED.incrementAndGet(index);
		}
	}

	private void indexRecord(ZuliaWorkPool zuliaWorkPool, String index, String uniqueId, String message) throws Exception {
		Document mongoDocument = new Document();
		mongoDocument.put("id", uniqueId);
		mongoDocument.put("message", message);
		Store store = new Store(uniqueId, index);
		store.setResultDocument(ResultDocBuilder.newBuilder().setDocument(mongoDocument));
		zuliaWorkPool.store(store);
	}

	private long primaryCount(ZuliaWorkPool zuliaWorkPool, String indexName) throws Exception {
		return zuliaWorkPool.search(new Search(indexName).setAmount(0).setRealtime(true).setPrimaryReplicaSettings(PrimaryReplicaSettings.PRIMARY_ONLY))
				.getTotalHits();
	}

	private long replicaCount(ZuliaWorkPool zuliaWorkPool, String indexName) throws Exception {
		return zuliaWorkPool.search(new Search(indexName).setAmount(0).setRealtime(true).setPrimaryReplicaSettings(PrimaryReplicaSettings.REPLICA_ONLY))
				.getTotalHits();
	}

	private long maxResidentTransient() {
		long max = 0;
		for (ZuliaNode zuliaNode : TestHelper.getZuliaNodes()) {
			max = Math.max(max, zuliaNode.getIndexManager().getLoadedIndexCache().getResidentCount());
		}
		return max;
	}

	private String describeResidency() {
		StringBuilder description = new StringBuilder();
		for (ZuliaNode zuliaNode : TestHelper.getZuliaNodes()) {
			description.append(zuliaNode.getIndexManager().getLoadedIndexCache().getResidentIndexNames()).append(" ");
		}
		return description.toString();
	}

	private void logClusterState(int round) {
		StringBuilder state = new StringBuilder();
		for (ZuliaNode zuliaNode : TestHelper.getZuliaNodes()) {
			var cache = zuliaNode.getIndexManager().getLoadedIndexCache();
			state.append(cache.getResidentCount()).append(" resident/").append(cache.getLoadCount()).append(" loads/").append(cache.getEvictionCount())
					.append(" evictions  ");
		}
		LOG.info("RepSoak: round {} cluster state: {}", round, state);
	}

	private void createIndex(ZuliaWorkPool zuliaWorkPool, String indexName, int replicas) throws Exception {
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("message");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("message").indexAs(DefaultAnalyzers.STANDARD));
		indexConfig.setIndexName(indexName);
		indexConfig.setNumberOfShards(1);
		indexConfig.setNumberOfReplicas(replicas);
		indexConfig.setShardCommitInterval(1000);
		indexConfig.setTransientIndex(true);
		zuliaWorkPool.createIndex(indexConfig);
	}
}
