package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.Sort;
import io.zulia.client.command.builder.StatFacet;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaQuery;
import io.zulia.server.test.node.shared.NodeExtension;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TopNLimitTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(3);

	private static final String INDEX_NAME = "topNLimitTest";
	private static final int SHARD_COUNT = 5;
	private static final int DOC_COUNT = 500;
	private static final int UNIQUE_CATEGORIES = 50;

	@Test
	@Order(1)
	public void createIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("category").indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("rating").index().sort());
		indexConfig.setIndexName(INDEX_NAME);
		indexConfig.setNumberOfShards(SHARD_COUNT);
		indexConfig.setShardCommitInterval(20);

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void indexData() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		for (int i = 0; i < DOC_COUNT; i++) {
			String category = "category_" + (i % UNIQUE_CATEGORIES);
			Document doc = new Document();
			doc.put("id", String.valueOf(i));
			doc.put("title", "Document " + i);
			doc.put("category", category);
			doc.put("rating", i % 100);

			Store store = new Store(String.valueOf(i), INDEX_NAME);
			store.setResultDocument(ResultDocBuilder.from(doc));
			zuliaWorkPool.store(store);
		}
	}

	@Test
	@Order(3)
	public void testDocumentResultLimit() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		int requestedAmount = 10;

		Search search = new Search(INDEX_NAME).setRealtime(true);
		search.setAmount(requestedAmount);

		SearchResult result = zuliaWorkPool.search(search);

		Assertions.assertEquals(DOC_COUNT, result.getTotalHits(), "Total hits should reflect all matching documents");
		Assertions.assertEquals(requestedAmount, result.getCompleteResults().size(),
				"Returned results should be exactly the requested amount");
	}

	@Test
	@Order(4)
	public void testDocumentResultLimitWithSort() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		int requestedAmount = 10;

		Search search = new Search(INDEX_NAME).setRealtime(true);
		search.addSort(new Sort("rating"));
		search.setAmount(requestedAmount);

		SearchResult result = zuliaWorkPool.search(search);

		Assertions.assertEquals(DOC_COUNT, result.getTotalHits(), "Total hits should reflect all matching documents");
		Assertions.assertEquals(requestedAmount, result.getCompleteResults().size(),
				"Returned results should be exactly the requested amount");
	}

	@Test
	@Order(5)
	public void testCountFacetTopNLimit() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		int topN = 10;

		// With default shardFacets (0 → maxFacets * 10 per shard)
		Search search = new Search(INDEX_NAME).setRealtime(true);
		search.addCountFacet(new CountFacet("category").setTopN(topN));

		SearchResult result = zuliaWorkPool.search(search);

		List<ZuliaQuery.FacetCount> facetCounts = result.getFacetCounts("category");
		Assertions.assertNotNull(facetCounts, "Facet counts should not be null");
		Assertions.assertEquals(topN, facetCounts.size(),
				"Count facet results (" + facetCounts.size() + ") should be exactly topN (" + topN + ")");

		// Verify counts are in descending order
		long previousCount = Long.MAX_VALUE;
		for (ZuliaQuery.FacetCount fc : facetCounts) {
			Assertions.assertTrue(fc.getCount() <= previousCount,
					"Facet counts should be in descending order: " + fc.getCount() + " should be <= " + previousCount);
			previousCount = fc.getCount();
		}
	}

	@Test
	@Order(6)
	public void testCountFacetTopNLimitWithShardFacets() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		int topN = 10;

		// With explicit shardFacets
		Search search = new Search(INDEX_NAME).setRealtime(true);
		search.addCountFacet(new CountFacet("category").setTopN(topN).setTopNShard(20));

		SearchResult result = zuliaWorkPool.search(search);

		List<ZuliaQuery.FacetCount> facetCounts = result.getFacetCounts("category");
		Assertions.assertNotNull(facetCounts, "Facet counts should not be null");
		Assertions.assertEquals(topN, facetCounts.size(),
				"Count facet results (" + facetCounts.size() + ") should be exactly topN (" + topN + ")");
	}

	@Test
	@Order(7)
	public void testCountFacetTopNLimitAllShardFacets() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		int topN = 10;

		// With shardFacets=-1 (all facets from each shard)
		Search search = new Search(INDEX_NAME).setRealtime(true);
		search.addCountFacet(new CountFacet("category").setTopN(topN).setTopNShard(-1));

		SearchResult result = zuliaWorkPool.search(search);

		List<ZuliaQuery.FacetCount> facetCounts = result.getFacetCounts("category");
		Assertions.assertNotNull(facetCounts, "Facet counts should not be null");
		Assertions.assertEquals(topN, facetCounts.size(),
				"Count facet results (" + facetCounts.size() + ") should be exactly topN (" + topN + ")");
	}

	@Test
	@Order(8)
	public void testStatFacetTopNLimit() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		int topN = 10;

		// With default shardFacets
		Search search = new Search(INDEX_NAME).setRealtime(true);
		search.addStat(new StatFacet("rating", "category").setTopN(topN));

		SearchResult result = zuliaWorkPool.search(search);

		List<ZuliaQuery.FacetStats> statFacets = result.getFacetFieldStat("rating", "category");
		Assertions.assertNotNull(statFacets, "Stat facets should not be null");
		Assertions.assertEquals(topN, statFacets.size(),
				"Stat facet results (" + statFacets.size() + ") should be exactly topN (" + topN + ")");
	}

	@Test
	@Order(9)
	public void testStatFacetTopNLimitWithShardFacets() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		int topN = 10;

		// With explicit shardFacets
		Search search = new Search(INDEX_NAME).setRealtime(true);
		search.addStat(new StatFacet("rating", "category").setTopN(topN).setTopNShard(20));

		SearchResult result = zuliaWorkPool.search(search);

		List<ZuliaQuery.FacetStats> statFacets = result.getFacetFieldStat("rating", "category");
		Assertions.assertNotNull(statFacets, "Stat facets should not be null");
		Assertions.assertEquals(topN, statFacets.size(),
				"Stat facet results (" + statFacets.size() + ") should be exactly topN (" + topN + ")");
	}

	@Test
	@Order(10)
	public void testStatFacetTopNLimitAllShardFacets() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		int topN = 10;

		// With shardFacets=-1 (all facets from each shard)
		Search search = new Search(INDEX_NAME).setRealtime(true);
		search.addStat(new StatFacet("rating", "category").setTopN(topN).setTopNShard(-1));

		SearchResult result = zuliaWorkPool.search(search);

		List<ZuliaQuery.FacetStats> statFacets = result.getFacetFieldStat("rating", "category");
		Assertions.assertNotNull(statFacets, "Stat facets should not be null");
		Assertions.assertEquals(topN, statFacets.size(),
				"Stat facet results (" + statFacets.size() + ") should be exactly topN (" + topN + ")");
	}

	@Test
	@Order(11)
	public void testVaryingTopN() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Test several topN values to ensure the limit is respected consistently
		for (int topN : new int[] { 1, 3, 5, 17, 49 }) {
			Search search = new Search(INDEX_NAME).setRealtime(true);
			search.addCountFacet(new CountFacet("category").setTopN(topN).setTopNShard(-1));
			search.addStat(new StatFacet("rating", "category").setTopN(topN).setTopNShard(-1));

			SearchResult result = zuliaWorkPool.search(search);


			List<ZuliaQuery.FacetCount> facetCounts = result.getFacetCounts("category");
			Assertions.assertNotNull(facetCounts);
			Assertions.assertEquals(topN, facetCounts.size(),
					"Count facets for topN=" + topN + ": expected " + topN + " but got " + facetCounts.size());

			List<ZuliaQuery.FacetStats> statFacets = result.getFacetFieldStat("rating", "category");
			Assertions.assertNotNull(statFacets);
			Assertions.assertEquals(topN, statFacets.size(),
					"Stat facets for topN=" + topN + ": expected " + topN + " but got " + statFacets.size());
		}
	}

	@Test
	@Order(12)
	public void testTopNExceedingCardinality() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Request more facets than exist — should return all unique values, not more
		int topN = UNIQUE_CATEGORIES + 100;

		Search search = new Search(INDEX_NAME).setRealtime(true);
		search.addCountFacet(new CountFacet("category").setTopN(topN).setTopNShard(-1));
		search.addStat(new StatFacet("rating", "category").setTopN(topN).setTopNShard(-1));

		SearchResult result = zuliaWorkPool.search(search);

		List<ZuliaQuery.FacetCount> facetCounts = result.getFacetCounts("category");
		Assertions.assertNotNull(facetCounts);
		Assertions.assertEquals(UNIQUE_CATEGORIES, facetCounts.size(),
				"Count facets should return all " + UNIQUE_CATEGORIES + " unique values when topN exceeds cardinality");

		List<ZuliaQuery.FacetStats> statFacets = result.getFacetFieldStat("rating", "category");
		Assertions.assertNotNull(statFacets);
		Assertions.assertEquals(UNIQUE_CATEGORIES, statFacets.size(),
				"Stat facets should return all " + UNIQUE_CATEGORIES + " unique values when topN exceeds cardinality");
	}
}
