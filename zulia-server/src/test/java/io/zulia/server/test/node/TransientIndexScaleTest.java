package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.GetIndexesResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.server.index.resident.LoadedIndexCache;
import io.zulia.server.test.node.shared.NodeExtension;
import io.zulia.server.test.node.shared.TestHelper;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Stress test for transient index residency
 * 100 transient indexes cycled against a cache bound of 10, so nearly every access cold loads and the evictor churns constantly.
 * Every phase asserts exact document counts, so a lost write, a query against a half-closed index, or a wrong reload surfaces as
 * a count mismatch rather than only as an exception.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TransientIndexScaleTest {

	private static final int INDEX_COUNT = 100;
	private static final int TRANSIENT_INDEX_COUNT = 10;
	private static final int SEED_DOCS = 5;
	private static final int WRITER_THREADS = 4;
	private static final int READER_THREADS = 2;

	private static final int CYCLE_ROUNDS = 3;
	private static final int DOCS_PER_CYCLE = 2;

	// the last VICTIM_COUNT indexes are excluded from the writers and instead rotate through
	// delete and recreate, one per round, to exercise the registry lifecycle under churn
	private static final int VICTIM_COUNT = 4;
	private static final int WRITABLE_COUNT = INDEX_COUNT - VICTIM_COUNT;

	// expected document count per index, shared across the ordered test methods
	private static final int[] EXPECTED = new int[INDEX_COUNT];

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(1, zuliaConfig -> zuliaConfig.setTransientIndexCacheSize(TRANSIENT_INDEX_COUNT));

	private static String indexName(int i) {
		return String.format("scale-%03d", i);
	}

	private static LoadedIndexCache cache() {
		return TestHelper.getZuliaNodes().getFirst().getIndexManager().getLoadedIndexCache();
	}

	@Test
	@Order(1)
	public void createIndexes() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		for (int i = 0; i < INDEX_COUNT; i++) {
			createIndex(zuliaWorkPool, i);
		}

		GetIndexesResult getIndexesResult = zuliaWorkPool.getIndexes();
		List<String> allIndexes = getIndexesResult.getIndexNames();
		for (int i = 0; i < INDEX_COUNT; i++) {
			Assertions.assertTrue(allIndexes.contains(indexName(i)), "index " + indexName(i) + " missing from index list");
		}
	}

	private void createIndex(ZuliaWorkPool zuliaWorkPool, int index) throws Exception {
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("message");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("message").indexAs(DefaultAnalyzers.STANDARD));
		indexConfig.setIndexName(indexName(index));
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20);
		indexConfig.setTransientIndex(true);
		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void seedAllIndexes() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		for (int i = 0; i < INDEX_COUNT; i++) {
			storeDocs(zuliaWorkPool, i, SEED_DOCS);
			Assertions.assertEquals(EXPECTED[i], count(zuliaWorkPool, indexName(i)), "wrong count in " + indexName(i) + " after seeding");
		}
	}

	@Test
	@Order(3)
	public void wildcardSweepSeesEverything() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		// faults ~90 unloaded indexes in through the parallel load path in one query
		Assertions.assertEquals(expectedTotal(), count(zuliaWorkPool, "scale-*"));
	}

	@Test
	@Order(4)
	public void concurrentCyclingKeepsCountsExact() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		LoadedIndexCache cache = cache();
		long loadsBefore = cache.getLoadCount();

		for (int round = 0; round < CYCLE_ROUNDS; round++) {
			// drain to the cap first so this round's accesses hit unloaded indexes and interleave with live evictions
			awaitResidentCountThreshold(cache);

			// rotate one victim per round: delete it (typically while unloaded), recreate it, reseed it.
			// Exercises delete and recreate racing the evictor and the registry lifecycle under churn.
			int victim = WRITABLE_COUNT + (round % VICTIM_COUNT);
			zuliaWorkPool.deleteIndex(indexName(victim));
			EXPECTED[victim] = 0;
			createIndex(zuliaWorkPool, victim);
			storeDocs(zuliaWorkPool, victim, SEED_DOCS);
			Assertions.assertEquals(SEED_DOCS, count(zuliaWorkPool, indexName(victim)), "wrong count in recreated " + indexName(victim));

			// per-index count bounds for this round: readers race the writers, so an owned index may hold
			// anywhere from its floor to floor + DOCS_PER_CYCLE at any instant, and never anything else
			int[] floors = EXPECTED.clone();
			int[] ceilings = floors.clone();
			for (int i = 0; i < WRITABLE_COUNT; i++) {
				ceilings[i] += DOCS_PER_CYCLE;
			}

			AtomicReference<Throwable> failure = new AtomicReference<>();
			AtomicBoolean writersDone = new AtomicBoolean();

			// each writer owns a disjoint quarter of the indexes, so per-index expected counts have a single writer
			List<Thread> writers = new ArrayList<>();
			for (int t = 0; t < WRITER_THREADS; t++) {
				int threadIndex = t;
				writers.add(Thread.ofVirtual().name("cycler-" + t).start(() -> {
					try {
						for (int i = threadIndex; i < WRITABLE_COUNT; i += WRITER_THREADS) {
							if (failure.get() != null) {
								return;
							}
							storeDocs(zuliaWorkPool, i, DOCS_PER_CYCLE);
							long hits = count(zuliaWorkPool, indexName(i));
							if (hits != EXPECTED[i]) {
								failure.compareAndSet(null, new AssertionError("expected " + EXPECTED[i] + " docs in " + indexName(i) + " but got " + hits));
							}
						}
					}
					catch (Throwable t2) {
						failure.compareAndSet(null, t2);
					}
				}));
			}

			// readers sweep every index while the writers and the evictor run, and counts must stay inside the bounds
			List<Thread> readers = new ArrayList<>();
			for (int r = 0; r < READER_THREADS; r++) {
				readers.add(Thread.ofVirtual().name("reader-" + r).start(() -> {
					try {
						while (!writersDone.get() && failure.get() == null) {
							for (int i = 0; i < WRITABLE_COUNT && !writersDone.get() && failure.get() == null; i++) {
								long hits = count(zuliaWorkPool, indexName(i));
								if (hits < floors[i] || hits > ceilings[i]) {
									failure.compareAndSet(null, new AssertionError(
											"count " + hits + " in " + indexName(i) + " outside bounds [" + floors[i] + ", " + ceilings[i] + "]"));
								}
							}
						}
					}
					catch (Throwable t2) {
						failure.compareAndSet(null, t2);
					}
				}));
			}

			// one wildcard sweep mid-round: the cluster-wide total must stay inside the round's bounds
			long wildcardHits = count(zuliaWorkPool, "scale-*");
			long floorSum = sum(floors);
			long ceilingSum = sum(ceilings);
			Assertions.assertTrue(wildcardHits >= floorSum && wildcardHits <= ceilingSum,
					"mid-round wildcard total " + wildcardHits + " outside bounds [" + floorSum + ", " + ceilingSum + "]");

			for (Thread writer : writers) {
				writer.join();
			}
			writersDone.set(true);
			for (Thread reader : readers) {
				reader.join();
			}
			int roundNumber = round;
			Assertions.assertNull(failure.get(), () -> "round " + roundNumber + " failed: " + failure.get());
		}

		// every drained round must reload most of the indexes cold, or the churn this test exists for never happened
		long coldLoads = cache.getLoadCount() - loadsBefore;
		Assertions.assertTrue(coldLoads >= (long) CYCLE_ROUNDS * (INDEX_COUNT - TRANSIENT_INDEX_COUNT) / 2,
				"expected heavy cold reloading during cycling but only saw " + coldLoads + " loads");
	}

	private static long sum(int[] counts) {
		long total = 0;
		for (int count : counts) {
			total += count;
		}
		return total;
	}

	private void awaitResidentCountThreshold(LoadedIndexCache cache) throws InterruptedException {
		long deadline = System.currentTimeMillis() + 90_000;
		while (System.currentTimeMillis() < deadline && cache.getResidentCount() > TRANSIENT_INDEX_COUNT) {
			Thread.sleep(500);
		}
		Assertions.assertTrue(cache.getResidentCount() <= TRANSIENT_INDEX_COUNT,
				"evictor failed to drain to the cap within 90s, resident: " + cache.getResidentIndexNames());
	}

	@Test
	@Order(5)
	public void wildcardSweepAfterChurn() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		Assertions.assertEquals(expectedTotal(), count(zuliaWorkPool, "scale-*"));
	}

	@Test
	@Order(6)
	public void residencyConvergesToCap() throws Exception {
		LoadedIndexCache cache = cache();

		long deadline = System.currentTimeMillis() + 180_000;
		while (System.currentTimeMillis() < deadline && cache.getResidentCount() > TRANSIENT_INDEX_COUNT) {
			Thread.sleep(1000);
		}

		Assertions.assertTrue(cache.getResidentCount() <= TRANSIENT_INDEX_COUNT,
				"expected at most " + TRANSIENT_INDEX_COUNT + " resident indexes after churn stopped but found " + cache.getResidentCount() + ": "
						+ cache.getResidentIndexNames());
		Assertions.assertTrue(cache.getEvictionCount() >= INDEX_COUNT - TRANSIENT_INDEX_COUNT,
				"expected heavy eviction churn but only saw " + cache.getEvictionCount() + " evictions");
	}

	@Test
	@Order(7)
	public void finalIntegritySweep() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		for (int i = 0; i < INDEX_COUNT; i++) {
			Assertions.assertEquals(EXPECTED[i], count(zuliaWorkPool, indexName(i)), "wrong count in " + indexName(i) + " on final sweep");
		}
	}

	private static long expectedTotal() {
		long total = 0;
		for (int expected : EXPECTED) {
			total += expected;
		}
		return total;
	}

	private long count(ZuliaWorkPool zuliaWorkPool, String indexName) throws Exception {
		return zuliaWorkPool.search(new Search(indexName).setAmount(0).setRealtime(true)).getTotalHits();
	}

	private void storeDocs(ZuliaWorkPool zuliaWorkPool, int index, int docCount) throws Exception {
		String indexName = indexName(index);
		for (int d = 0; d < docCount; d++) {
			String uniqueId = indexName + "-doc-" + EXPECTED[index];
			Document mongoDocument = new Document();
			mongoDocument.put("id", uniqueId);
			mongoDocument.put("message", "message " + EXPECTED[index]);
			Store store = new Store(uniqueId, indexName);
			store.setResultDocument(ResultDocBuilder.newBuilder().setDocument(mongoDocument));
			zuliaWorkPool.store(store);
			EXPECTED[index]++;
		}
	}
}
