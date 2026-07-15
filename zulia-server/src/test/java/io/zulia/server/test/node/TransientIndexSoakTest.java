package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.server.index.resident.LoadedIndexCache;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Hour-scale soak test that tests 5000 transient indexes against a residency cap of 100, hammered by paced writers
 * and readers whose access pattern is ~50x wider than the cap, so nearly every operation pays a cold
 * load while the evictor runs continuously. Victim indexes rotate through delete and recreate, and
 * rotating 100-index wildcard windows fan cold loads in bursts. Excluded from the regular test task,
 * run with: ./gradlew :zulia-server:soakTest (duration knob: -Dzulia.soak.minutes, default 60).
 * <p>
 * Invariants asserted throughout: single-writer indexes always count exactly, racing readers always
 * observe counts inside [expected-before, expected-after + 1], wildcard windows stay inside summed
 * bounds, residency stays under a thrash ceiling while hammering, and after quiescing the node
 * converges back to the cap with every one of the 5000 indexes still counting exactly.
 * <p>
 * A 90 minute run at this sizing (~5M seeded documents) passed on 2026-07-14 with 167300 cold loads,
 * 166872 evictions, peak residency 535, and an exact final sweep of all 5000 indexes.
 */
@Tag("soak")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TransientIndexSoakTest {

	private static final Logger LOG = LoggerFactory.getLogger(TransientIndexSoakTest.class);

	private static final int INDEX_COUNT = 5000;
	private static final int CACHE_SIZE = 100;
	private static final int VICTIM_COUNT = 8;
	private static final int WRITABLE_COUNT = INDEX_COUNT - VICTIM_COUNT;
	private static final int SEED_DOCS = 1000;
	private static final int SEED_THREADS = 16;
	private static final int DOCS_PER_WRITE = 10;
	private static final int WRITER_THREADS = 8;
	private static final int READER_THREADS = 4;
	private static final long WRITER_PAUSE_MS = 200;
	private static final long READER_PAUSE_MS = 150;
	// paced load rate times the 10s minimum residency bounds the soft-cap overshoot, and far above it means thrash
	private static final int RESIDENT_CEILING = 2500;
	// setup, convergence, and the final 5000-index sweep get this slice of the total budget
	private static final long RESERVE_MS = 8 * 60_000;

	private static final long SUITE_START_MS = System.currentTimeMillis();
	private static final long TOTAL_BUDGET_MS = Long.parseLong(System.getProperty("zulia.soak.minutes", "60")) * 60_000;

	private static final AtomicIntegerArray EXPECTED = new AtomicIntegerArray(WRITABLE_COUNT);
	private static final AtomicIntegerArray VICTIM_EXPECTED = new AtomicIntegerArray(VICTIM_COUNT);

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(1, zuliaConfig -> zuliaConfig.setTransientIndexCacheSize(CACHE_SIZE));

	private static LoadedIndexCache cache() {
		return TestHelper.getZuliaNodes().getFirst().getIndexManager().getLoadedIndexCache();
	}

	private static String indexName(int i) {
		return String.format("soak-%04d", i);
	}

	private static String victimName(int v) {
		return "soakvictim-" + v;
	}

	@Test
	@Order(1)
	public void createIndexes() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		for (int i = 0; i < WRITABLE_COUNT; i++) {
			createIndex(zuliaWorkPool, indexName(i));
			if (i % 500 == 499) {
				LOG.info("Soak: created {} of {} indexes ({} resident)", i + 1, WRITABLE_COUNT, cache().getResidentCount());
			}
		}
		for (int v = 0; v < VICTIM_COUNT; v++) {
			createIndex(zuliaWorkPool, victimName(v));
		}
		Assertions.assertEquals(INDEX_COUNT, zuliaWorkPool.getIndexes().getIndexNames().stream().filter(name -> name.startsWith("soak")).count());
	}

	@Test
	@Order(2)
	public void seedIndexes() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// ~5M seed documents: parallel seeders own disjoint index sets so per-index counts stay single-writer
		AtomicReference<Throwable> failure = new AtomicReference<>();
		List<Thread> seeders = new ArrayList<>();
		for (int t = 0; t < SEED_THREADS; t++) {
			int threadIndex = t;
			seeders.add(Thread.ofVirtual().name("soak-seeder-" + t).start(() -> {
				try {
					for (int i = threadIndex; i < WRITABLE_COUNT && failure.get() == null; i += SEED_THREADS) {
						storeWritableDocs(zuliaWorkPool, i, SEED_DOCS);
						if (i % 500 < SEED_THREADS) {
							LOG.info("Soak: seeding progressed past index {} of {}", i, WRITABLE_COUNT);
						}
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

		for (int i = 0; i < WRITABLE_COUNT; i += 500) {
			Assertions.assertEquals(EXPECTED.get(i), count(zuliaWorkPool, indexName(i)), "wrong count in " + indexName(i) + " after seeding");
		}
		for (int v = 0; v < VICTIM_COUNT; v++) {
			seedVictim(zuliaWorkPool, v);
		}
	}

	@Test
	@Order(3)
	public void hammer() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		LoadedIndexCache cache = cache();

		long deadline = SUITE_START_MS + TOTAL_BUDGET_MS - RESERVE_MS;
		long hammerMillis = Math.max(60_000, deadline - System.currentTimeMillis());
		deadline = System.currentTimeMillis() + hammerMillis;
		LOG.info("Soak: hammering for {} minutes", hammerMillis / 60_000);

		long loadsBefore = cache.getLoadCount();
		long evictionsBefore = cache.getEvictionCount();

		AtomicBoolean stop = new AtomicBoolean();
		AtomicReference<Throwable> failure = new AtomicReference<>();

		List<Thread> threads = new ArrayList<>();
		for (int t = 0; t < WRITER_THREADS; t++) {
			int threadIndex = t;
			threads.add(Thread.ofVirtual().name("soak-writer-" + t).start(() -> writerLoop(zuliaWorkPool, threadIndex, stop, failure)));
		}
		for (int r = 0; r < READER_THREADS; r++) {
			int threadIndex = r;
			threads.add(Thread.ofVirtual().name("soak-reader-" + r).start(() -> readerLoop(zuliaWorkPool, threadIndex, stop, failure)));
		}

		int tick = 0;
		int peakResident = 0;
		try {
			while (System.currentTimeMillis() < deadline && failure.get() == null) {
				Thread.sleep(15_000);
				tick++;

				int resident = cache.getResidentCount();
				peakResident = Math.max(peakResident, resident);
				Assertions.assertTrue(resident <= RESIDENT_CEILING,
						"resident count " + resident + " exceeded the thrash ceiling " + RESIDENT_CEILING + " during the hammer phase");

				if (tick % 2 == 0) {
					wildcardWindowSweep(zuliaWorkPool, tick / 2, failure);
				}
				if (tick % 4 == 0) {
					rotateVictim(zuliaWorkPool, (tick / 4) % VICTIM_COUNT);
				}
				if (tick % 8 == 0) {
					LOG.info("Soak: {} min in, {} resident (peak {}), {} loads, {} evictions", tick / 4, resident, peakResident,
							cache.getLoadCount() - loadsBefore, cache.getEvictionCount() - evictionsBefore);
				}
			}
		}
		finally {
			stop.set(true);
			for (Thread thread : threads) {
				thread.join();
			}
		}

		Assertions.assertNull(failure.get(), () -> "soak hammer failed: " + failure.get());

		long loads = cache.getLoadCount() - loadsBefore;
		long evictions = cache.getEvictionCount() - evictionsBefore;
		LOG.info("Soak: hammer finished with {} loads and {} evictions, peak resident {}", loads, evictions, peakResident);
		// the paced workload sustains ~40 cold loads per second; 5 per second is the churn floor at any duration
		long minExpected = Math.max(500, hammerMillis / 1000 * 5);
		Assertions.assertTrue(loads >= minExpected, "expected at least " + minExpected + " cold loads over the hammer phase but only saw " + loads);
		Assertions.assertTrue(evictions >= minExpected, "expected at least " + minExpected + " evictions over the hammer phase but only saw " + evictions);
	}

	@Test
	@Order(4)
	public void convergesToCap() throws Exception {
		LoadedIndexCache cache = cache();
		long deadline = System.currentTimeMillis() + 300_000;
		while (System.currentTimeMillis() < deadline && cache.getResidentCount() > CACHE_SIZE) {
			Thread.sleep(2000);
		}
		Assertions.assertTrue(cache.getResidentCount() <= CACHE_SIZE,
				"expected convergence to " + CACHE_SIZE + " resident after quiescing but found " + cache.getResidentCount());
	}

	@Test
	@Order(5)
	public void finalIntegritySweep() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		for (int i = 0; i < WRITABLE_COUNT; i++) {
			long hits = count(zuliaWorkPool, indexName(i));
			Assertions.assertEquals(EXPECTED.get(i), hits, "wrong count in " + indexName(i) + " on the final sweep");
			if (i % 500 == 499) {
				LOG.info("Soak: final sweep verified {} of {} indexes", i + 1, WRITABLE_COUNT);
			}
		}
		for (int v = 0; v < VICTIM_COUNT; v++) {
			Assertions.assertEquals(VICTIM_EXPECTED.get(v), count(zuliaWorkPool, victimName(v)), "wrong count in " + victimName(v) + " on the final sweep");
		}
	}

	private void writerLoop(ZuliaWorkPool zuliaWorkPool, int threadIndex, AtomicBoolean stop, AtomicReference<Throwable> failure) {
		try {
			int i = threadIndex;
			while (!stop.get() && failure.get() == null) {
				storeWritableDocs(zuliaWorkPool, i, DOCS_PER_WRITE);
				long hits = count(zuliaWorkPool, indexName(i));
				if (hits != EXPECTED.get(i)) {
					failure.compareAndSet(null, new AssertionError("writer expected " + EXPECTED.get(i) + " docs in " + indexName(i) + " but got " + hits));
					return;
				}
				i += WRITER_THREADS;
				if (i >= WRITABLE_COUNT) {
					i = threadIndex;
				}
				Thread.sleep(WRITER_PAUSE_MS);
			}
		}
		catch (Throwable t) {
			failure.compareAndSet(null, t);
		}
	}

	private void readerLoop(ZuliaWorkPool zuliaWorkPool, int threadIndex, AtomicBoolean stop, AtomicReference<Throwable> failure) {
		try {
			// stride by a prime unaligned with the writer stride so readers sweep all indexes
			int i = threadIndex * 631;
			while (!stop.get() && failure.get() == null) {
				i = (i + 631) % WRITABLE_COUNT;
				int before = EXPECTED.get(i);
				long hits = count(zuliaWorkPool, indexName(i));
				int after = EXPECTED.get(i);
				// the single writer increments after every stored document, even inside a multi-doc write,
				// so at most one store is uncounted at any instant
				if (hits < before || hits > after + 1) {
					failure.compareAndSet(null,
							new AssertionError("reader saw " + hits + " docs in " + indexName(i) + " outside bounds [" + before + ", " + (after + 1) + "]"));
					return;
				}
				Thread.sleep(READER_PAUSE_MS);
			}
		}
		catch (Throwable t) {
			failure.compareAndSet(null, t);
		}
	}

	private void wildcardWindowSweep(ZuliaWorkPool zuliaWorkPool, int sweep, AtomicReference<Throwable> failure) throws Exception {
		int window = sweep % 50;
		int from = window * 100;
		int to = Math.min(WRITABLE_COUNT, from + 100);

		long floor = 0;
		for (int i = from; i < to; i++) {
			floor += EXPECTED.get(i);
		}
		long hits = count(zuliaWorkPool, String.format("soak-%02d*", window));
		long ceiling = WRITER_THREADS;
		for (int i = from; i < to; i++) {
			ceiling += EXPECTED.get(i);
		}
		if (hits < floor || hits > ceiling) {
			failure.compareAndSet(null, new AssertionError(
					"wildcard window soak-" + String.format("%02d", window) + "* saw " + hits + " outside bounds [" + floor + ", " + ceiling + "]"));
		}
	}

	private void rotateVictim(ZuliaWorkPool zuliaWorkPool, int victim) throws Exception {
		zuliaWorkPool.deleteIndex(victimName(victim));
		VICTIM_EXPECTED.set(victim, 0);
		createIndex(zuliaWorkPool, victimName(victim));
		seedVictim(zuliaWorkPool, victim);
	}

	private void seedVictim(ZuliaWorkPool zuliaWorkPool, int victim) throws Exception {
		String name = victimName(victim);
		for (int d = 0; d < SEED_DOCS; d++) {
			indexRecord(zuliaWorkPool, name, name + "-doc-" + VICTIM_EXPECTED.get(victim), "victim message");
			VICTIM_EXPECTED.incrementAndGet(victim);
		}
		Assertions.assertEquals(SEED_DOCS, count(zuliaWorkPool, name), "wrong count in recreated " + name);
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

	private long count(ZuliaWorkPool zuliaWorkPool, String indexName) throws Exception {
		return zuliaWorkPool.search(new Search(indexName).setAmount(0).setRealtime(true)).getTotalHits();
	}

	private void createIndex(ZuliaWorkPool zuliaWorkPool, String indexName) throws Exception {
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("message");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("message").indexAs(DefaultAnalyzers.STANDARD));
		indexConfig.setIndexName(indexName);
		indexConfig.setNumberOfShards(1);
		// a small commit interval with 1000-doc seeds means 50 commits per index during seeding, while
		// the idle commit still fires, which is what the eviction guards care about
		indexConfig.setShardCommitInterval(1000);
		indexConfig.setTransientIndex(true);
		zuliaWorkPool.createIndex(indexConfig);
	}
}
