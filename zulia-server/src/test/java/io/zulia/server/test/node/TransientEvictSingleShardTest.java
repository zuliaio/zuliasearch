package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaIndex.IndexShardMapping;
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

import java.util.List;

/**
 * Tests idle eviction of a single-shard unreplicated transient index on a multi-node cluster. The only
 * shard lives on one node, so eviction happens there while cluster requests keep arriving on either node.
 * Queries and stores routed through the cluster must fault the index back in on the hosting node.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TransientEvictSingleShardTest {

	private static final String STABLE_INDEX = "soloStable";
	private static final String TRANSIENT_INDEX = "soloTransient";
	private static final int DOCS = 50;

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(2, zuliaConfig -> zuliaConfig.setTransientIndexIdleTimeoutSeconds(15));

	@Test
	@Order(1)
	public void createAndIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		createIndex(zuliaWorkPool, STABLE_INDEX, false);
		createIndex(zuliaWorkPool, TRANSIENT_INDEX, true);

		for (String indexName : List.of(STABLE_INDEX, TRANSIENT_INDEX)) {
			for (int i = 0; i < DOCS; i++) {
				indexRecord(zuliaWorkPool, indexName, indexName + "-" + i, "message " + i);
			}
			Assertions.assertEquals(DOCS, count(zuliaWorkPool, indexName));
		}

		IndexShardMapping mapping = anyCache().getRegistered(TRANSIENT_INDEX).shardMapping();
		Assertions.assertEquals(1, mapping.getShardMappingCount(), "this suite tests a single-shard index");
		Assertions.assertTrue(residentOn(hostingNode(), TRANSIENT_INDEX), "the hosting node must have the index resident after indexing");
	}

	@Test
	@Order(2)
	public void evictsOnHostingNodeWhileIdle() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		awaitEvictedEverywhere();

		Assertions.assertTrue(hostingNode().getIndexManager().getLoadedIndexCache().getEvictionCount() >= 1,
				"the node hosting the only shard must run the eviction");
		for (ZuliaNode zuliaNode : TestHelper.getZuliaNodes()) {
			Assertions.assertTrue(residentOn(zuliaNode, STABLE_INDEX), "non-transient index must stay resident on every node");
		}

		// a query routed through the cluster faults the index back in on the hosting node
		Assertions.assertEquals(DOCS, count(zuliaWorkPool, TRANSIENT_INDEX));
		Assertions.assertTrue(residentOn(hostingNode(), TRANSIENT_INDEX), "querying an evicted index must reload it on the hosting node");
	}

	@Test
	@Order(3)
	public void storeFaultsInEvictedPrimary() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		awaitEvictedEverywhere();

		// the write path routes to the primary shard and must fault the evicted index back in
		indexRecord(zuliaWorkPool, TRANSIENT_INDEX, TRANSIENT_INDEX + "-extra", "extra message");
		Assertions.assertEquals(DOCS + 1, count(zuliaWorkPool, TRANSIENT_INDEX));
		Assertions.assertTrue(residentOn(hostingNode(), TRANSIENT_INDEX), "storing into an evicted index must reload it on the hosting node");
	}

	@Test
	@Order(4)
	public void lazyLoadAfterRestartAcrossCluster() throws Exception {
		nodeExtension.restartNodes();
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		for (ZuliaNode zuliaNode : TestHelper.getZuliaNodes()) {
			Assertions.assertTrue(residentOn(zuliaNode, STABLE_INDEX), "non-transient index loads at startup on every node");
			Assertions.assertFalse(residentOn(zuliaNode, TRANSIENT_INDEX), "transient index must not load at startup on any node");
		}

		Assertions.assertEquals(DOCS + 1, count(zuliaWorkPool, TRANSIENT_INDEX), "the write from before the restart must survive eviction and shutdown");
		Assertions.assertTrue(residentOn(hostingNode(), TRANSIENT_INDEX));
	}

	private void awaitEvictedEverywhere() throws Exception {
		long deadline = System.currentTimeMillis() + 120_000;
		while (System.currentTimeMillis() < deadline && residentAnywhere()) {
			Thread.sleep(1000);
		}
		Assertions.assertFalse(residentAnywhere(), "single-shard transient index should evict after the idle timeout");
	}

	private boolean residentAnywhere() {
		return TestHelper.getZuliaNodes().stream().anyMatch(node -> residentOn(node, TRANSIENT_INDEX));
	}

	private boolean residentOn(ZuliaNode zuliaNode, String indexName) {
		return zuliaNode.getIndexManager().getLoadedIndexCache().getResidentIndexNames().contains(indexName);
	}

	private LoadedIndexCache anyCache() {
		return TestHelper.getZuliaNodes().getFirst().getIndexManager().getLoadedIndexCache();
	}

	private ZuliaNode hostingNode() {
		Node primary = anyCache().getRegistered(TRANSIENT_INDEX).shardMapping().getShardMapping(0).getPrimaryNode();
		return TestHelper.getZuliaNodes().stream().filter(node -> ZuliaNode.isEqual(primary, ZuliaNode.nodeFromConfig(node.getZuliaConfig()))).findFirst()
				.orElseThrow(() -> new IllegalStateException("no test node matches primary " + primary.getServerAddress() + ":" + primary.getServicePort()));
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
