package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
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

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TransientReplicatedIndexTest {

	private static final String REPLICATED_INDEX = "transientReplicated";
	private static final String SOLO_INDEX = "transientSolo";
	private static final int DOCS_PER_INDEX = 30;

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(2, zuliaConfig -> {
		zuliaConfig.setTransientIndexCacheSize(1);
		zuliaConfig.setTransientIndexIdleTimeoutSeconds(15);
	});

	@Test
	@Order(1)
	public void createAndIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		createIndex(zuliaWorkPool, REPLICATED_INDEX, 1);
		createIndex(zuliaWorkPool, SOLO_INDEX, 0);

		for (String indexName : List.of(REPLICATED_INDEX, SOLO_INDEX)) {
			for (int i = 0; i < DOCS_PER_INDEX; i++) {
				indexRecord(zuliaWorkPool, indexName, indexName + "-" + i, "message " + i);
			}
			Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, indexName));
		}
	}

	@Test
	@Order(2)
	public void replicatedTransientIndexIsExemptFromEviction() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// idle timeout is 15s: the unreplicated transient index gets evicted everywhere, the replicated one never does
		long deadline = System.currentTimeMillis() + 120_000;
		while (System.currentTimeMillis() < deadline && soloResidentOnANode()) {
			Thread.sleep(1000);
		}

		Assertions.assertFalse(soloResidentOnANode(), "unreplicated transient index should be evicted after the idle timeout");
		for (ZuliaNode zuliaNode : TestHelper.getZuliaNodes()) {
			Assertions.assertTrue(zuliaNode.getIndexManager().getLoadedIndexCache().getResidentIndexNames().contains(REPLICATED_INDEX),
					"an index with replicas must not be evicted while eviction of replicated indexes is disabled");
		}

		// both still serve correct results (the evicted one reloads on demand)
		Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, REPLICATED_INDEX));
		Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, SOLO_INDEX));
	}

	private boolean soloResidentOnANode() {
		return TestHelper.getZuliaNodes().stream().anyMatch(node -> node.getIndexManager().getLoadedIndexCache().getResidentIndexNames().contains(SOLO_INDEX));
	}

	private long count(ZuliaWorkPool zuliaWorkPool, String indexName) throws Exception {
		return zuliaWorkPool.search(new Search(indexName).setAmount(0).setRealtime(true)).getTotalHits();
	}

	private void createIndex(ZuliaWorkPool zuliaWorkPool, String indexName, int replicas) throws Exception {
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("message");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("message").indexAs(DefaultAnalyzers.STANDARD));
		indexConfig.setIndexName(indexName);
		indexConfig.setNumberOfShards(1);
		indexConfig.setNumberOfReplicas(replicas);
		indexConfig.setShardCommitInterval(20);
		indexConfig.setTransientIndex(true);
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
