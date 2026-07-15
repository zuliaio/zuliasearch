package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.UpdateIndex;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.UpdateIndexResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaBase;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TransientIndexLoaderTest {

	private static final String STABLE_INDEX = "stableIndex";
	private static final String TRANSIENT_A = "transientA";
	private static final String TRANSIENT_B = "transientB";
	private static final String ALIAS_A = "transientAliasA";
	private static final int DOCS_PER_INDEX = 50;

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(1, zuliaConfig -> zuliaConfig.setTransientIndexCacheSize(1));

	private static LoadedIndexCache cache() {
		return TestHelper.getZuliaNodes().getFirst().getIndexManager().getLoadedIndexCache();
	}

	@Test
	@Order(1)
	public void createAndIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		createIndex(zuliaWorkPool, STABLE_INDEX, false);
		createIndex(zuliaWorkPool, TRANSIENT_A, true);
		createIndex(zuliaWorkPool, TRANSIENT_B, true);
		zuliaWorkPool.createIndexAlias(ALIAS_A, TRANSIENT_A);

		for (String indexName : List.of(STABLE_INDEX, TRANSIENT_A, TRANSIENT_B)) {
			for (int i = 0; i < DOCS_PER_INDEX; i++) {
				indexRecord(zuliaWorkPool, indexName, indexName + "-" + i, "message " + i);
			}
			Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, indexName));
		}
	}

	@Test
	@Order(2)
	public void evictionUnderSizeBound() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// touch A so B is the longest-idle transient index
		Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, TRANSIENT_A));

		LoadedIndexCache cache = cache();
		long deadline = System.currentTimeMillis() + 90_000;
		while (System.currentTimeMillis() < deadline && cache.getEvictionCount() == 0) {
			Thread.sleep(1000);
		}
		Assertions.assertTrue(cache.getEvictionCount() >= 1, "expected an eviction with cache size 1, resident: " + cache.getResidentIndexNames());
		Assertions.assertTrue(cache.getResidentIndexNames().contains(STABLE_INDEX), "non-transient index must never be evicted");

		// evicted transient indexes reload on demand with correct results
		Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, TRANSIENT_A));
		Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, TRANSIENT_B));
	}

	@Test
	@Order(3)
	public void concurrentAccessDuringEvictionChurn() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		AtomicReference<Throwable> failure = new AtomicReference<>();
		long end = System.currentTimeMillis() + 20_000;

		List<Thread> threads = new ArrayList<>();
		for (int i = 0; i < 6; i++) {
			String indexName = (i % 2 == 0) ? TRANSIENT_A : TRANSIENT_B;
			threads.add(Thread.ofVirtual().name("hammer-" + i).start(() -> {
				try {
					while (System.currentTimeMillis() < end && failure.get() == null) {
						long hits = count(zuliaWorkPool, indexName);
						if (hits != DOCS_PER_INDEX) {
							failure.compareAndSet(null, new AssertionError("expected " + DOCS_PER_INDEX + " docs in " + indexName + " but got " + hits));
						}
					}
				}
				catch (Throwable t) {
					failure.compareAndSet(null, t);
				}
			}));
		}
		for (Thread thread : threads) {
			thread.join();
		}
		Assertions.assertNull(failure.get(), () -> "query failed during eviction churn: " + failure.get());
	}

	@Test
	@Order(4)
	public void lazyLoadAfterRestart() throws Exception {
		nodeExtension.restartNodes();
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		LoadedIndexCache cache = cache();
		List<String> resident = cache.getResidentIndexNames();
		Assertions.assertTrue(resident.contains(STABLE_INDEX), "non-transient index loads at startup");
		Assertions.assertFalse(resident.contains(TRANSIENT_A), "transient index must not load at startup");
		Assertions.assertFalse(resident.contains(TRANSIENT_B), "transient index must not load at startup");

		// stats report every registered index with its residency, without loading the unloaded ones
		Map<String, Boolean> residentByIndex = TestHelper.getZuliaNodes().getFirst().getIndexManager().getIndexStats().stream()
				.collect(Collectors.toMap(ZuliaBase.IndexStats::getIndexName, ZuliaBase.IndexStats::getResident));
		Assertions.assertEquals(Boolean.TRUE, residentByIndex.get(STABLE_INDEX));
		Assertions.assertEquals(Boolean.FALSE, residentByIndex.get(TRANSIENT_A));
		Assertions.assertEquals(Boolean.FALSE, residentByIndex.get(TRANSIENT_B));
		Assertions.assertFalse(cache.getResidentIndexNames().contains(TRANSIENT_A), "stats must not load an unloaded transient index");

		// settings are readable without loading the index
		Assertions.assertEquals(Boolean.TRUE, zuliaWorkPool.getIndexConfig(TRANSIENT_A).getIndexConfig().getTransientIndex());
		Assertions.assertFalse(cache.getResidentIndexNames().contains(TRANSIENT_A), "reading settings must not load the index");

		// an alias to an unloaded transient index faults it in on query
		Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, ALIAS_A));
		Assertions.assertTrue(cache.getResidentIndexNames().contains(TRANSIENT_A), "alias query must load the aliased transient index");

		// a wildcard query faults the remaining unloaded transient index in and sees every document
		long hits = zuliaWorkPool.search(new Search("transient*").setAmount(0).setRealtime(true)).getTotalHits();
		Assertions.assertEquals(2L * DOCS_PER_INDEX, hits);
		Assertions.assertTrue(cache.getResidentIndexNames().contains(TRANSIENT_B));
	}

	@Test
	@Order(5)
	public void deleteUnloadedTransientIndex() throws Exception {
		nodeExtension.restartNodes();
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		Assertions.assertFalse(cache().getResidentIndexNames().contains(TRANSIENT_B), "transient index must not load at startup");

		zuliaWorkPool.deleteIndex(TRANSIENT_B);

		Path shardDir = Path.of("/tmp/zuliaTest/node0/indexes/" + TRANSIENT_B + "_0_idx");
		Assertions.assertFalse(Files.exists(shardDir), "deleting an unloaded transient index must remove its on-disk shard data");
		Assertions.assertFalse(zuliaWorkPool.getIndexes().getIndexNames().contains(TRANSIENT_B));

		// the remaining indexes are unaffected
		Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, STABLE_INDEX));
		Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, TRANSIENT_A));
	}

	@Test
	@Order(6)
	public void flipIndexToTransientByUpdate() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// the same partial-update path zuliaadmin updateIndex --transientIndex true uses
		UpdateIndexResult updateResult = zuliaWorkPool.updateIndex(new UpdateIndex(STABLE_INDEX).setTransientIndex(true));
		Assertions.assertTrue(updateResult.getFullIndexSettings().getTransientIndex(), "updateIndex must persist the transient flag");

		nodeExtension.restartNodes();
		Assertions.assertFalse(cache().getResidentIndexNames().contains(STABLE_INDEX), "index flipped to transient must not load at startup");
		Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, STABLE_INDEX), "flipped index must lazy-load with correct results");
		Assertions.assertTrue(cache().getResidentIndexNames().contains(STABLE_INDEX));

		// flip back: the index loads at startup and is pinned resident again
		zuliaWorkPool.updateIndex(new UpdateIndex(STABLE_INDEX).setTransientIndex(false));
		nodeExtension.restartNodes();
		Assertions.assertTrue(cache().getResidentIndexNames().contains(STABLE_INDEX), "index flipped back to non-transient must load at startup");
		Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, STABLE_INDEX));
	}

	private long count(ZuliaWorkPool zuliaWorkPool, String indexName) throws Exception {
		return zuliaWorkPool.search(new Search(indexName).setAmount(0).setRealtime(true)).getTotalHits();
	}

	private void createIndex(ZuliaWorkPool zuliaWorkPool, String indexName, boolean transientIndex) throws Exception {
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("message");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("message").indexAs(DefaultAnalyzers.STANDARD));
		indexConfig.setIndexName(indexName);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20);
		indexConfig.setTransientIndex(transientIndex);
		zuliaWorkPool.createIndex(indexConfig);
	}

	private void indexRecord(ZuliaWorkPool zuliaWorkPool, String index, String uniqueId, String message) throws Exception {
		Document mongoDocument = new Document();
		mongoDocument.put("id", uniqueId);
		mongoDocument.put("message", message);
		Store store = new Store(uniqueId, index);
		store.setResultDocument(ResultDocBuilder.newBuilder().setDocument(mongoDocument));
		zuliaWorkPool.store(store);
	}
}
