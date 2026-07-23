package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaIndex.ShardMapping;
import io.zulia.server.index.resident.LoadedIndexCache;
import io.zulia.server.node.ZuliaNode;
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Tests eviction of unreplicated transient indexes across a multi-node cluster. Every index has one
 * primary shard on each node, each node's cache evicts independently under its own size bound, and a
 * federated query must fault an evicted index back in on every node that hosts a shard.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TransientEvictMultiNodeTest {

	private static final String STABLE_INDEX = "multiNodeStable";
	private static final String TRANSIENT_A = "multiNodeTransientA";
	private static final String TRANSIENT_B = "multiNodeTransientB";
	private static final int DOCS_PER_INDEX = 50;

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(2, zuliaConfig -> zuliaConfig.setTransientIndexCacheSize(1));

	@Test
	@Order(1)
	public void createAndIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		createIndex(zuliaWorkPool, STABLE_INDEX, false);
		createIndex(zuliaWorkPool, TRANSIENT_A, true);
		createIndex(zuliaWorkPool, TRANSIENT_B, true);

		for (String indexName : List.of(STABLE_INDEX, TRANSIENT_A, TRANSIENT_B)) {
			for (int i = 0; i < DOCS_PER_INDEX; i++) {
				indexRecord(zuliaWorkPool, indexName, indexName + "-" + i, "message " + i);
			}
			Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, indexName));
		}

		// the premise of this suite: every two-shard index has a primary on each of the two nodes
		LoadedIndexCache anyCache = TestHelper.getZuliaNodes().getFirst().getIndexManager().getLoadedIndexCache();
		for (String indexName : List.of(STABLE_INDEX, TRANSIENT_A, TRANSIENT_B)) {
			List<Node> primaries = anyCache.getRegistered(indexName).shardMapping().getShardMappingList().stream().map(ShardMapping::getPrimaryNode).toList();
			Assertions.assertEquals(2, primaries.size());
			Assertions.assertFalse(ZuliaNode.isEqual(primaries.get(0), primaries.get(1)), indexName + " must have a primary shard on each node");
		}
	}

	@Test
	@Order(2)
	public void everyNodeEvictsIndependently() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// touch A on both nodes so B is the longest-idle transient index everywhere
		Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, TRANSIENT_A));

		long deadline = System.currentTimeMillis() + 120_000;
		while (System.currentTimeMillis() < deadline && !everyNodeSettledUnderBound()) {
			Thread.sleep(1000);
		}
		Assertions.assertTrue(everyNodeSettledUnderBound(), "expected each node to evict down to its size bound of 1, resident by node: " + residentByNode());

		for (ZuliaNode zuliaNode : TestHelper.getZuliaNodes()) {
			LoadedIndexCache cache = zuliaNode.getIndexManager().getLoadedIndexCache();
			Assertions.assertTrue(cache.getEvictionCount() >= 1, "each node must run its own eviction");
			Assertions.assertTrue(cache.getResidentIndexNames().contains(STABLE_INDEX), "non-transient index must never be evicted");
		}

		// evicted indexes reload on demand with results federated from both nodes
		Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, TRANSIENT_A));
		Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, TRANSIENT_B));
		long wildcardHits = zuliaWorkPool.search(new Search("multiNodeTransient*").setAmount(0).setRealtime(true)).getTotalHits();
		Assertions.assertEquals(2L * DOCS_PER_INDEX, wildcardHits);
	}

	@Test
	@Order(3)
	public void concurrentFederatedAccessDuringEvictionChurn() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		AtomicReference<Throwable> failure = new AtomicReference<>();
		long end = System.currentTimeMillis() + 20_000;

		List<Thread> threads = new ArrayList<>();
		for (int i = 0; i < 6; i++) {
			String indexName = (i % 2 == 0) ? TRANSIENT_A : TRANSIENT_B;
			threads.add(Thread.ofVirtual().name("federated-hammer-" + i).start(() -> {
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
		Assertions.assertNull(failure.get(), () -> "federated query failed during eviction churn: " + failure.get());
	}

	@Test
	@Order(4)
	public void federatedQueryFaultsInOnEveryNode() throws Exception {
		nodeExtension.restartNodes();
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		for (ZuliaNode zuliaNode : TestHelper.getZuliaNodes()) {
			List<String> resident = zuliaNode.getIndexManager().getLoadedIndexCache().getResidentIndexNames();
			Assertions.assertTrue(resident.contains(STABLE_INDEX), "non-transient index loads at startup on every node");
			Assertions.assertFalse(resident.contains(TRANSIENT_A), "transient index must not load at startup");
			Assertions.assertFalse(resident.contains(TRANSIENT_B), "transient index must not load at startup");
		}

		Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, TRANSIENT_B));

		for (ZuliaNode zuliaNode : TestHelper.getZuliaNodes()) {
			Assertions.assertTrue(zuliaNode.getIndexManager().getLoadedIndexCache().getResidentIndexNames().contains(TRANSIENT_B),
					"a federated query must fault the transient index in on every node that hosts a shard");
		}
	}

	private boolean everyNodeSettledUnderBound() {
		for (ZuliaNode zuliaNode : TestHelper.getZuliaNodes()) {
			LoadedIndexCache cache = zuliaNode.getIndexManager().getLoadedIndexCache();
			if (cache.getEvictionCount() < 1) {
				return false;
			}
			long residentTransient = cache.getResidentIndexNames().stream().filter(name -> TRANSIENT_A.equals(name) || TRANSIENT_B.equals(name)).count();
			if (residentTransient > 1) {
				return false;
			}
		}
		return true;
	}

	private Map<String, List<String>> residentByNode() {
		return TestHelper.getZuliaNodes().stream().collect(Collectors.toMap(node -> node.getZuliaConfig().getServerAddress() + ":" + node.getZuliaConfig().getServicePort(),
				node -> node.getIndexManager().getLoadedIndexCache().getResidentIndexNames()));
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
		indexConfig.setNumberOfShards(2);
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
