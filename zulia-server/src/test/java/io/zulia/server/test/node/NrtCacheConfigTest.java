package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.UpdateIndex;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.server.test.node.shared.NodeExtension;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * End-to-end coverage of configurable NRT cache sizes: an index created with custom (small) NRT sizes indexes and
 * searches correctly, and disabling NRT caching live via UpdateIndex (no restart) does not break search-after-write.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class NrtCacheConfigTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(1);

	private static final String INDEX_NAME = "nrtCacheConfigTest";
	private static final int FIRST_BATCH = 50;
	private static final int SECOND_BATCH = 50;

	@Test
	@Order(1)
	public void createIndexWithCustomNrt() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD));
		indexConfig.setIndexName(INDEX_NAME);
		indexConfig.setNumberOfShards(1);
		// Small, non-default NRT cache sizes - the index must still index and search correctly.
		indexConfig.setNrtIndexMaxMergeSizeMB(8);
		indexConfig.setNrtIndexMaxCachedMB(16);
		indexConfig.setNrtTaxoMaxCachedMB(4);

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void storeAndSearch() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		for (int i = 0; i < FIRST_BATCH; i++) {
			store(zuliaWorkPool, i);
		}

		SearchResult result = zuliaWorkPool.search(new Search(INDEX_NAME).setRealtime(true));
		Assertions.assertEquals(FIRST_BATCH, result.getTotalHits(), "all stored docs should be searchable with custom NRT sizes");
	}

	@Test
	@Order(3)
	public void disableNrtCachingLiveThenStoreAndSearch() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Live setting change - no node restart or index reload by the caller.
		zuliaWorkPool.updateIndex(new UpdateIndex(INDEX_NAME).setNrtCachingDisabled(true));

		for (int i = FIRST_BATCH; i < FIRST_BATCH + SECOND_BATCH; i++) {
			store(zuliaWorkPool, i);
		}

		SearchResult result = zuliaWorkPool.search(new Search(INDEX_NAME).setRealtime(true));
		Assertions.assertEquals(FIRST_BATCH + SECOND_BATCH, result.getTotalHits(),
				"search-after-write must still work after disabling NRT caching live");
	}

	private static void store(ZuliaWorkPool zuliaWorkPool, int id) throws Exception {
		Document document = new Document();
		document.put("id", String.valueOf(id));
		document.put("title", "doc " + id);

		Store store = new Store(String.valueOf(id), INDEX_NAME);
		store.setResultDocument(ResultDocBuilder.newBuilder().setDocument(document));
		zuliaWorkPool.store(store);
	}
}
