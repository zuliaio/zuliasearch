package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
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
 * A response above shardQueryCacheMaxResponseMB is returned but not kept, while smaller responses stay cached as before.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CacheMaxResponseTest {

	@RegisterExtension
	static final RestNodeExtension nodeExtension = new RestNodeExtension(1); // stats are read over REST

	private static final String INDEX = "cacheMaxResponse";
	private static final int DOCS = 4_000;
	private static final String PADDING = "x".repeat(300);

	@Test
	@Order(1)
	public void createIndex() throws Exception {
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("label");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("label").indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("bucket").index().facet());
		indexConfig.setIndexName(INDEX);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardQueryCacheMaxResponseMB(1);
		nodeExtension.getGrpcClient().createIndex(indexConfig);

		Assertions.assertThrows(Exception.class, () -> {
			ClientIndexConfig negative = new ClientIndexConfig();
			negative.addDefaultSearchField("label");
			negative.addFieldConfig(FieldConfigBuilder.createString("label").indexAs(DefaultAnalyzers.LC_KEYWORD));
			negative.setIndexName(INDEX + "Negative");
			negative.setNumberOfShards(1);
			negative.setShardQueryCacheMaxResponseMB(-1);
			nodeExtension.getGrpcClient().createIndex(negative);
		}, "a negative limit is rejected at creation");
	}

	@Test
	@Order(2)
	public void index() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getGrpcClient();
		for (int i = 0; i < DOCS; i++) {
			// every label is distinct and long, so an unlimited facet on it returns more than the 1 MB limit from one shard
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

		Search small = new Search(INDEX).setAmount(0).addQuery(new FilterQuery("bucket:3")).addCountFacet(new CountFacet("bucket")).addDistinctCount("label");
		Assertions.assertEquals(0, zuliaWorkPool.search(small).getShardsCached());
		SearchResult smallAgain = zuliaWorkPool.search(small);
		Assertions.assertEquals(DOCS / 10, smallAgain.getTotalHits());
		Assertions.assertEquals(DOCS / 10, smallAgain.getDistinctCount("label").getCount());
		Assertions.assertEquals(1, smallAgain.getShardsCached(), "a response of a few KB stays in the cache");

		Search huge = new Search(INDEX).setAmount(0).addCountFacet(new CountFacet("label").setTopN(-1).setTopNShard(-1));
		SearchResult first = zuliaWorkPool.search(huge);
		Assertions.assertEquals(DOCS, first.getFacetCounts("label").size());
		SearchResult again = zuliaWorkPool.search(huge);
		Assertions.assertEquals(DOCS, again.getFacetCounts("label").size());
		Assertions.assertEquals(0, again.getShardsCached(), "a response above the limit is dropped as soon as it is computed");

		CacheStats general = zuliaWorkPool.getStats().getNodeStats().getIndexStatList().stream().filter(s -> s.getIndexName().equals(INDEX)).findFirst()
				.map(IndexStats::getShardCacheStatList).orElseThrow().getFirst().getGeneralCache();
		Assertions.assertTrue(general.getResultSize() < 1024 * 1024, "the cache holds less than the 1 MB limit: " + general);
		Assertions.assertTrue(general.getEstimatedSize() >= 1, "the small response is still there: " + general);
		Assertions.assertEquals(2, general.getOversizedResponseCount(), "both runs of the oversized search are counted: " + general);
	}
}
