package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.UpdateIndex;
import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaBase.CacheStats;
import io.zulia.message.ZuliaBase.IndexStats;
import io.zulia.server.test.node.shared.RestNodeExtension;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * A shard query cache bounded by MB keeps small responses and lets one that is larger than the whole budget go.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CacheBytesTest {

	@RegisterExtension
	static final RestNodeExtension nodeExtension = new RestNodeExtension(1); // stats are read over REST

	private static final String INDEX = "cacheBytes";
	private static final int DOCS = 4_000;
	private static final String PADDING = "x".repeat(300);

	@Test
	@Order(1)
	public void createIndex() throws Exception {
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("label");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("label").indexAs(DefaultAnalyzers.LC_KEYWORD).facet());
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("bucket").index().facet());
		indexConfig.setIndexName(INDEX);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardQueryCacheMB(1);
		nodeExtension.getGrpcClient().createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void index() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getGrpcClient();
		for (int i = 0; i < DOCS; i++) {
			// every label is distinct and long, so an unlimited facet on it returns more than the 1 MB budget from one shard
			Document document = new Document().append("id", "d" + i).append("label", "label-" + i + "-" + PADDING).append("bucket", i % 10);
			zuliaWorkPool.store(new Store("d" + i, INDEX).setResultDocument(ResultDocBuilder.newBuilder().setDocument(document)));
		}
		// a realtime search commits the shard so the cached searches below see every document
		Assertions.assertEquals(DOCS, zuliaWorkPool.search(new Search(INDEX).setAmount(0).setRealtime(true)).getTotalHits());
	}

	@Test
	@Order(3)
	public void smallResponsesAreCachedAndAnOversizedOneIsNot() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getGrpcClient();

		Search small = new Search(INDEX).setAmount(0).addQuery(new FilterQuery("bucket:3")).addCountFacet(new CountFacet("bucket"));
		Assertions.assertEquals(0, zuliaWorkPool.search(small).getShardsCached());
		SearchResult smallAgain = zuliaWorkPool.search(small);
		Assertions.assertEquals(DOCS / 10, smallAgain.getTotalHits());
		Assertions.assertEquals(1, smallAgain.getShardsCached(), "a response of a few hundred bytes stays in the cache");

		Search huge = new Search(INDEX).setAmount(0).addCountFacet(new CountFacet("label").setTopN(-1).setTopNShard(-1));
		SearchResult first = zuliaWorkPool.search(huge);
		Assertions.assertEquals(DOCS, first.getFacetCounts("label").size());
		// Caffeine evicts an over-weight entry on its maintenance thread, so a slow machine may need a moment before the miss shows
		SearchResult again = zuliaWorkPool.search(huge);
		for (int attempt = 0; attempt < 20 && again.getShardsCached() != 0; attempt++) {
			Thread.sleep(50);
			again = zuliaWorkPool.search(huge);
		}
		Assertions.assertEquals(DOCS, again.getFacetCounts("label").size());
		Assertions.assertEquals(0, again.getShardsCached(), "a response above the whole budget is not kept");

		CacheStats general = nodeExtension.getGrpcClient().getStats().getNodeStats().getIndexStatList().stream().filter(s -> s.getIndexName().equals(INDEX))
				.findFirst().map(IndexStats::getShardCacheStatList).orElseThrow().getFirst().getGeneralCache();
		Assertions.assertTrue(general.getEvictionCount() >= 1, "the oversized response was evicted: " + general);
		Assertions.assertTrue(general.getResultSize() < 1024 * 1024, "the cache holds less than its 1 MB budget: " + general);
		Assertions.assertTrue(general.getEstimatedSize() >= 1, "the small response is still there: " + general);
	}

	@Test
	@Order(4)
	public void changedCacheSettingsApplyToAnIdleIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getGrpcClient();
		Search huge = new Search(INDEX).setAmount(0).addCountFacet(new CountFacet("label").setTopN(-1).setTopNShard(-1));

		// nothing has been written since the last search, so the reader has nothing to reopen and only the rebuilt cache can apply the new bounds
		zuliaWorkPool.updateIndex(new UpdateIndex(INDEX).setShardQueryCacheMB(16));
		zuliaWorkPool.search(huge);
		Assertions.assertEquals(1, zuliaWorkPool.search(huge).getShardsCached(), "a 16 MB budget keeps the response that the 1 MB budget evicted");

		zuliaWorkPool.updateIndex(new UpdateIndex(INDEX).setShardQueryCacheMaxResponseMB(1));
		zuliaWorkPool.search(huge);
		Assertions.assertEquals(0, zuliaWorkPool.search(huge).getShardsCached(), "a 1 MB response limit drops it again");
	}
}
