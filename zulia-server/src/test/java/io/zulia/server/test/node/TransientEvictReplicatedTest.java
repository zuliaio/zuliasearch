package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Tests eviction of replicated transient indexes with transientIndexEvictReplicated enabled.
 * The replicated index must actually evict on idle, reload on demand, and its replica must catch up with
 * writes made after the reload.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TransientEvictReplicatedTest {

	private static final String INDEX_NAME = "transientEvictableReplicated";
	private static final int INITIAL_DOCS = 30;
	private static final int SECOND_WAVE_DOCS = 10;

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(2, zuliaConfig -> {
		zuliaConfig.setTransientIndexIdleTimeoutSeconds(15);
		zuliaConfig.setTransientIndexEvictReplicated(true);
	});

	@Test
	@Order(1)
	public void createAndIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("message");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("message").indexAs(DefaultAnalyzers.STANDARD));
		indexConfig.setIndexName(INDEX_NAME);
		indexConfig.setNumberOfShards(1);
		indexConfig.setNumberOfReplicas(1);
		indexConfig.setShardCommitInterval(20);
		indexConfig.setTransientIndex(true);
		zuliaWorkPool.createIndex(indexConfig);

		for (int i = 0; i < INITIAL_DOCS; i++) {
			indexRecord(zuliaWorkPool, INDEX_NAME + "-" + i, "message " + i);
		}
		Assertions.assertEquals(INITIAL_DOCS, countDocsForIndex(zuliaWorkPool));
	}

	@Test
	@Order(2)
	public void replicatedIndexEvictsOnIdle() throws Exception {
		long deadline = System.currentTimeMillis() + 120_000;
		while (System.currentTimeMillis() < deadline && indexIsResidentOnNode()) {
			Thread.sleep(1000);
		}
		Assertions.assertFalse(indexIsResidentOnNode(), "replicated transient index should evict on idle when transientIndexEvictReplicated is enabled");

		long evictions = TestHelper.getZuliaNodes().stream().mapToLong(node -> node.getIndexManager().getLoadedIndexCache().getEvictionCount()).sum();
		Assertions.assertTrue(evictions >= 1, "expected at least one eviction across the cluster");
	}

	@Test
	@Order(3)
	public void reloadServesWritesAndReplicaCatchesUp() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// writes fault the evicted primary back in and succeed
		for (int i = 0; i < SECOND_WAVE_DOCS; i++) {
			indexRecord(zuliaWorkPool, INDEX_NAME + "-second-" + i, "second wave " + i);
		}
		int expected = INITIAL_DOCS + SECOND_WAVE_DOCS;
		Assertions.assertEquals(expected, countDocsForIndex(zuliaWorkPool));

		// the replica catches up after the post-write commit is pushed (idle commit plus replication)
		long deadline = System.currentTimeMillis() + 120_000;
		long replicaHits = -1;
		while (System.currentTimeMillis() < deadline) {
			replicaHits = zuliaWorkPool.search(
					new Search(INDEX_NAME).setAmount(0).setRealtime(true).setPrimaryReplicaSettings(PrimaryReplicaSettings.REPLICA_ONLY)).getTotalHits();
			if (replicaHits == expected) {
				break;
			}
			Thread.sleep(1000);
		}
		Assertions.assertEquals(expected, replicaHits, "replica must catch up with writes made after the primary was evicted and reloaded");
	}

	private boolean indexIsResidentOnNode() {
		for (ZuliaNode zuliaNode : TestHelper.getZuliaNodes()) {
			if (zuliaNode.getIndexManager().getLoadedIndexCache().getResidentIndexNames().contains(INDEX_NAME)) {
				return true;
			}
		}
		return false;
	}

	private long countDocsForIndex(ZuliaWorkPool zuliaWorkPool) throws Exception {
		return zuliaWorkPool.search(new Search(INDEX_NAME).setAmount(0).setRealtime(true)).getTotalHits();
	}

	private void indexRecord(ZuliaWorkPool zuliaWorkPool, String uniqueId, String message) throws Exception {
		Document mongoDocument = new Document();
		mongoDocument.put("id", uniqueId);
		mongoDocument.put("message", message);
		Store store = new Store(uniqueId, INDEX_NAME);
		store.setResultDocument(ResultDocBuilder.newBuilder().setDocument(mongoDocument));
		zuliaWorkPool.store(store);
	}
}
