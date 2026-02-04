package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.MatchAllQuery;
import io.zulia.client.command.builder.NumericStat;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.StatFacet;
import io.zulia.client.command.factory.FilterFactory;
import io.zulia.client.command.factory.RangeBehavior;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaQuery.FacetStats;
import io.zulia.server.test.node.shared.NodeExtension;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.opentest4j.AssertionFailedError;

import java.util.ArrayList;
import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class StatTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(3);

	public static final String STAT_TEST_INDEX = "stats";

	private static final int repeatCount = 100;
	private static final int shardCount = 5;

	@Test
	@Order(1)
	public void createIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("pathFacet").indexAs(DefaultAnalyzers.LC_KEYWORD).facetHierarchical().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("normalFacet").indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("unusedFacet").indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort());
		//indexConfig.addFieldConfig(FieldConfigBuilder.create("authorCount", FieldType.NUMERIC_INT).index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createDouble("rating").index().sort());
		indexConfig.setIndexName(STAT_TEST_INDEX);
		indexConfig.setNumberOfShards(shardCount);
		indexConfig.setShardCommitInterval(20); //force some commits

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void index() throws Exception {

		for (int i = 0; i < repeatCount; i++) {
			int uniqueDocs = 7;
			indexRecord(i * uniqueDocs, "something special", "top1/middle/bottom1", "foo", 3, List.of(3.5, 1.0));
			indexRecord(i * uniqueDocs + 1, "something really special", "top1/middle/bottom2", "foo", 4, List.of(2.5));
			indexRecord(i * uniqueDocs + 2, "something even more special", "top1/middle/bottom3", "foo", 2, null);
			indexRecord(i * uniqueDocs + 3, "something special", "top2/middle/bottom3", "bar", 2, List.of(0.5));
			indexRecord(i * uniqueDocs + 4, "something really special", "top3/middle/bottom4", "bar", 5, List.of(3.0));
			indexRecord(i * uniqueDocs + 5, "something really special", "top3/middle/bottom4", null, 4, List.of());
			indexRecord(i * uniqueDocs + 6, "boring", "top4/middle/bottom5", "other", 1, List.of(0.0));
		}
	}

	private void indexRecord(int id, String title, String pathFacet, String normalFacet, int authorCount, List<Double> rating) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		String uniqueId = "" + id;

		Document mongoDocument = new Document();
		mongoDocument.put("id", uniqueId);
		mongoDocument.put("title", title);
		mongoDocument.put("pathFacet", pathFacet);
		mongoDocument.put("normalFacet", normalFacet);
		mongoDocument.put("authorCount", authorCount);
		mongoDocument.put("rating", rating);

		Store s = new Store(uniqueId, STAT_TEST_INDEX);
		s.setResultDocument(ResultDocBuilder.from(mongoDocument));
		zuliaWorkPool.store(s);

	}

	@Test
	@Order(3)
	public void statTest() throws Exception {
		statTestSearch(null);
		statTestSearch(2);
	}

	private void statTestSearch(Integer concurrency) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		List<Double> percentiles = new ArrayList<>() {{
			add(0.0);
			add(0.25);
			add(0.50);
			add(0.75);
			add(1.0);
		}};

		Search search;
		SearchResult searchResult;

		//run the first search with realtime to force seeing latest changes without waiting for a commit
		search = new Search(STAT_TEST_INDEX).setRealtime(true);
		if (concurrency != null) {
			search.setConcurrency(concurrency);
		}

		search.addStat(new NumericStat("rating").setPercentiles(percentiles));
		search.addQuery(new FilterQuery("title:boring").exclude());

		searchResult = zuliaWorkPool.search(search);
		ratingTest(searchResult.getNumericFieldStat("rating"));

		search.clearStat();
		search.addStat(new StatFacet("rating", "normalFacet").setPercentiles(percentiles));
		searchResult = zuliaWorkPool.search(search);
		ratingNormalTest(searchResult);

		search.clearStat();
		search.addStat(new NumericStat("rating").setPercentiles(percentiles));
		search.addStat(new StatFacet("rating", "normalFacet").setPercentiles(percentiles));
		searchResult = zuliaWorkPool.search(search);
		ratingTest(searchResult.getNumericFieldStat("rating"));
		ratingNormalTest(searchResult);

		search.clearStat();
		search.addStat(new StatFacet("rating", "pathFacet").setPercentiles(percentiles).setTopN(2).setTopNShard(2));
		searchResult = zuliaWorkPool.search(search);

		ratingPathTest(searchResult);

		search.clearStat();
		search.addStat(new StatFacet("rating", "normalFacet").setPercentiles(percentiles));
		search.addStat(new StatFacet("rating", "pathFacet").setPercentiles(percentiles));
		searchResult = zuliaWorkPool.search(search);
		ratingNormalTest(searchResult);
		ratingPathTest(searchResult);

		search = new Search(STAT_TEST_INDEX);
		search.addStat(new StatFacet("authorCount", "pathFacet").setPercentiles(percentiles));
		Search finalSearch = search;
		Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(finalSearch),
				"Expecting: Search: Numeric field <authorCount> must be indexed as a SORTABLE numeric field");

		search = new Search(STAT_TEST_INDEX);
		search.addStat(new StatFacet("rating", "normalFacet").setPercentiles(percentiles));
		search.addQuery(new FilterQuery("title:boring"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());
		List<FacetStats> ratingByFacet = searchResult.getFacetFieldStat("rating", "normalFacet");
		Assertions.assertEquals(1, ratingByFacet.size());
		Assertions.assertEquals(repeatCount, ratingByFacet.getFirst().getDocCount());
		Assertions.assertEquals(0, ratingByFacet.getFirst().getMin().getDoubleValue());
		Assertions.assertEquals(0, ratingByFacet.getFirst().getMax().getDoubleValue());
		Assertions.assertEquals(0, ratingByFacet.getFirst().getSum().getDoubleValue());

		search = new Search(STAT_TEST_INDEX);
		search.addStat(new StatFacet("rating", "madeUp"));
		Search finalSearch2 = search;
		Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(finalSearch2), "Expecting: madeUp is not defined as a facetable field");




		search = new Search(STAT_TEST_INDEX);
		search.addStat(new StatFacet("rating", "unusedFacet"));
		searchResult = zuliaWorkPool.search(search);
		List<FacetStats> unusedFacet = searchResult.getFacetFieldStat("rating", "unusedFacet");
		Assertions.assertEquals(0, unusedFacet.size());
	}

	private void ratingTest(FacetStats ratingStat) {
		Assertions.assertEquals(0.5, ratingStat.getMin().getDoubleValue(), 0.001);
		Assertions.assertEquals(3.5, ratingStat.getMax().getDoubleValue(), 0.001);

		Assertions.assertEquals(10.5 * repeatCount, ratingStat.getSum().getDoubleValue(), 0.001);
		Assertions.assertEquals(4L * repeatCount, ratingStat.getDocCount());
		Assertions.assertEquals(6L * repeatCount, ratingStat.getAllDocCount());
		Assertions.assertEquals(5L * repeatCount, ratingStat.getValueCount());

		Assertions.assertEquals(5, ratingStat.getPercentilesCount());

		double precision = 0.001;
		Assertions.assertEquals(0.5, ratingStat.getPercentiles(0).getValue(), precision * 0.5);
		Assertions.assertEquals(1.0, ratingStat.getPercentiles(1).getValue(), precision * 1.0);
		Assertions.assertEquals(2.5, ratingStat.getPercentiles(2).getValue(), precision * 2.5);
		Assertions.assertEquals(3.0, ratingStat.getPercentiles(3).getValue(), precision * 3.0);
		Assertions.assertEquals(3.5, ratingStat.getPercentiles(4).getValue(), precision * 3.5);
	}

	private void ratingNormalTest(SearchResult searchResult) {
		List<FacetStats> ratingByFacet = searchResult.getFacetFieldStat("rating", "normalFacet");

		for (FacetStats facetStats : ratingByFacet) {
			if (facetStats.getFacet().equals("foo")) {
				Assertions.assertEquals(1, facetStats.getMin().getDoubleValue(), 0.001);
				Assertions.assertEquals(3.5, facetStats.getMax().getDoubleValue(), 0.001);
				Assertions.assertEquals(7L * repeatCount, facetStats.getSum().getDoubleValue(), 0.001);
				Assertions.assertEquals(2L * repeatCount, facetStats.getDocCount());
				Assertions.assertEquals(3L * repeatCount, facetStats.getAllDocCount());
				Assertions.assertEquals(3L * repeatCount, facetStats.getValueCount());
			}
			else if (facetStats.getFacet().equals("bar")) {
				Assertions.assertEquals(0.5, facetStats.getMin().getDoubleValue(), 0.001);
				Assertions.assertEquals(3.0, facetStats.getMax().getDoubleValue(), 0.001);
				Assertions.assertEquals(3.5 * repeatCount, facetStats.getSum().getDoubleValue(), 0.001);
				Assertions.assertEquals(2L * repeatCount, facetStats.getDocCount());
				Assertions.assertEquals(2L * repeatCount, facetStats.getAllDocCount());
				Assertions.assertEquals(2L * repeatCount, facetStats.getValueCount());
			}
			else {
				throw new AssertionFailedError("Unexpect facet <" + facetStats.getFacet() + ">");
			}
		}
	}

	private void ratingPathTest(SearchResult searchResult) {
		List<FacetStats> ratingByPathFacet = searchResult.getFacetFieldStat("rating", "pathFacet");

		for (FacetStats facetStats : ratingByPathFacet) {
			if (facetStats.getFacet().equals("top1")) {
				Assertions.assertEquals(1, facetStats.getMin().getDoubleValue(), 0.001);
				Assertions.assertEquals(3.5, facetStats.getMax().getDoubleValue(), 0.001);
				Assertions.assertEquals(7L * repeatCount, facetStats.getSum().getDoubleValue(), 0.001);
				Assertions.assertEquals(2L * repeatCount, facetStats.getDocCount());
				Assertions.assertEquals(3L * repeatCount, facetStats.getAllDocCount());
				Assertions.assertEquals(3L * repeatCount, facetStats.getValueCount());
			}
			else if (facetStats.getFacet().equals("top2")) {
				Assertions.assertEquals(0.5, facetStats.getMin().getDoubleValue(), 0.001);
				Assertions.assertEquals(0.5, facetStats.getMax().getDoubleValue(), 0.001);
				Assertions.assertEquals(0.5 * repeatCount, facetStats.getSum().getDoubleValue(), 0.001);
				Assertions.assertEquals(repeatCount, facetStats.getDocCount());
				Assertions.assertEquals(repeatCount, facetStats.getAllDocCount());
				Assertions.assertEquals(repeatCount, facetStats.getValueCount());
			}
			else if (facetStats.getFacet().equals("top3")) {
				Assertions.assertEquals(3.0, facetStats.getMin().getDoubleValue(), 0.001);
				Assertions.assertEquals(3.0, facetStats.getMax().getDoubleValue(), 0.001);
				Assertions.assertEquals(3.0 * repeatCount, facetStats.getSum().getDoubleValue(), 0.001);
				Assertions.assertEquals(repeatCount, facetStats.getDocCount());
				Assertions.assertEquals(repeatCount, facetStats.getValueCount());
			}
			else {
				throw new AssertionFailedError("Unexpect facet <" + facetStats.getFacet() + ">");
			}
		}
	}

	@Test
	@Order(4)
	public void testRangeFilters() throws Exception {
		testRangeFiltersSearch(null);
		testRangeFiltersSearch(3);
	}

	private static void testRangeFiltersSearch(Integer concurrency) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		SearchResult searchResult;
		Search search = new Search(STAT_TEST_INDEX);
		if (concurrency != null) {
			search.setConcurrency(concurrency);
		}
		// Get how many are in the dataset
		search.addQuery(new MatchAllQuery());
		searchResult = zuliaWorkPool.search(search);
		search.clearQueries();
		Assertions.assertEquals(700, searchResult.getTotalHits());

		// Find all documents with any value in rating stat
		search.addQuery(FilterFactory.rangeDouble("rating").toQuery());
		searchResult = zuliaWorkPool.search(search);
		search.clearQueries();
		Assertions.assertEquals(500, searchResult.getTotalHits());

		// Find documents without any value in rating stat
		search.addQuery(FilterFactory.rangeDouble("rating").setExclude().toQuery());
		searchResult = zuliaWorkPool.search(search);
		search.clearQueries();
		Assertions.assertEquals(200, searchResult.getTotalHits());

		// Test max only
		search.clearQueries();
		search.addQuery(FilterFactory.rangeDouble("rating").setMaxValue(0.50).toQuery());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(200, searchResult.getTotalHits());

		// Inverted max only
		search.clearQueries();
		search.addQuery(FilterFactory.rangeDouble("rating").setMaxValue(0.50).setExclude().toQuery());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(500, searchResult.getTotalHits());

		// Test inclusivity
		search.clearQueries();
		search.addQuery(FilterFactory.rangeDouble("rating").setMaxValue(0.50).setEndpointBehavior(RangeBehavior.EXCLUSIVE).toQuery());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(100, searchResult.getTotalHits());

		// Test endpoints
		search.clearQueries();
		search.addQuery(FilterFactory.rangeDouble("rating").setMinValue(0.0).setMaxValue(3.5).setEndpointBehavior(RangeBehavior.INCLUSIVE).toQuery());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(500, searchResult.getTotalHits());

		// Test exclude endpoints
		search.clearQueries();
		search.addQuery(FilterFactory.rangeDouble("rating").setMinValue(0.0).setMaxValue(3.5).setEndpointBehavior(RangeBehavior.EXCLUSIVE).toQuery());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(400, searchResult.getTotalHits());
	}

	@Test
	@Order(5)
	public void reindex() throws Exception {

		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("pathFacet").indexAs(DefaultAnalyzers.LC_KEYWORD).facetHierarchical().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("normalFacet").indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("authorCount").index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createDouble("rating").index().sort());
		indexConfig.setIndexName(STAT_TEST_INDEX);
		indexConfig.setNumberOfShards(shardCount);
		indexConfig.setShardCommitInterval(20); //force some commits

		zuliaWorkPool.createIndex(indexConfig);

		//trigger indexing again with path2 added in the index config
		index();

	}

	@Test
	@Order(6)
	public void restart() throws Exception {
		nodeExtension.restartNodes();
	}

	@Test
	@Order(7)
	public void confirm() throws Exception {
		confirmSearches(1);
		confirmSearches(3);
	}

	private void confirmSearches(int concurrency) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		Search search = new Search(STAT_TEST_INDEX);
		search.setConcurrency(concurrency);
		search.addQuery(new FilterQuery("title:boring").exclude());
		search.addStat(new NumericStat("authorCount"));

		SearchResult searchResult = zuliaWorkPool.search(search);
		authorCountTest(searchResult);

		search.clearStat();
		search.addStat(new StatFacet("authorCount", "normalFacet"));
		searchResult = zuliaWorkPool.search(search);

		authorCountNormalFacetTest(searchResult);

		search.clearStat();
		search.addStat(new StatFacet("authorCount", "pathFacet"));
		searchResult = zuliaWorkPool.search(search);
		authorCountPathFacetTest(searchResult);

		search.clearStat();
		search.addStat(new NumericStat("authorCount"));
		search.addStat(new StatFacet("authorCount", "normalFacet"));
		search.addStat(new StatFacet("authorCount", "pathFacet"));
		searchResult = zuliaWorkPool.search(search);
		authorCountTest(searchResult);
		authorCountNormalFacetTest(searchResult);
		authorCountPathFacetTest(searchResult);

		search.clearStat();
		search.addStat(new StatFacet("authorCount", "pathFacet"));
		search.addStat(new NumericStat("authorCount"));
		search.addStat(new StatFacet("authorCount", "normalFacet"));
		search.addStat(new StatFacet("rating", "normalFacet"));
		search.addStat(new StatFacet("rating", "pathFacet"));
		searchResult = zuliaWorkPool.search(search);
		authorCountTest(searchResult);
		authorCountNormalFacetTest(searchResult);
		authorCountPathFacetTest(searchResult);
		ratingNormalTest(searchResult);
		ratingPathTest(searchResult);

		search = new Search(STAT_TEST_INDEX);
		search.setConcurrency(concurrency);
		search.addStat(new StatFacet("rating", "pathFacet"));
		search.addQuery(new FilterQuery("\"something really special\"").addQueryField("title"));
		search.addQuery(new FilterQuery("authorCount:4"));
		search.addQuery(new FilterQuery("rating:[2 TO 3]").exclude());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());
		List<FacetStats> ratingByFacet = searchResult.getFacetFieldStat("rating", "pathFacet");
		Assertions.assertEquals(1, ratingByFacet.size());
		Assertions.assertEquals(0, ratingByFacet.getFirst().getDocCount()); //No docs have values for rating that match even though there are facets for pathFacet
		System.err.println(ratingByFacet.getFirst().getMin().getDoubleValue());
		System.err.println(ratingByFacet.getFirst().getMax().getDoubleValue());
		System.err.println(ratingByFacet.getFirst().getSum().getDoubleValue());
		Assertions.assertEquals(Double.POSITIVE_INFINITY, ratingByFacet.getFirst().getMin().getDoubleValue(),
				0.001); // Positive Infinity for Min when no values found
		Assertions.assertEquals(Double.NEGATIVE_INFINITY, ratingByFacet.getFirst().getMax().getDoubleValue(),
				0.001); // Negative Infinity for Max when no values found
		Assertions.assertEquals(0, ratingByFacet.getFirst().getSum().getDoubleValue(), 0.001);
	}

	private void authorCountPathFacetTest(SearchResult searchResult) {
		List<FacetStats> authorCountPathFacet = searchResult.getFacetFieldStat("authorCount", "pathFacet");
		for (FacetStats facetStats : authorCountPathFacet) {
			if (facetStats.getFacet().equals("top1")) {
				Assertions.assertEquals(2L, facetStats.getMin().getLongValue());
				Assertions.assertEquals(4L, facetStats.getMax().getLongValue());
				Assertions.assertEquals(9L * repeatCount, facetStats.getSum().getLongValue());
				Assertions.assertEquals(3L * repeatCount, facetStats.getDocCount());
				Assertions.assertEquals(3L * repeatCount, facetStats.getAllDocCount());
				Assertions.assertEquals(3L * repeatCount, facetStats.getValueCount());
			}
			else if (facetStats.getFacet().equals("top2")) {
				Assertions.assertEquals(2L, facetStats.getMin().getLongValue());
				Assertions.assertEquals(2L, facetStats.getMax().getLongValue());
				Assertions.assertEquals(2L * repeatCount, facetStats.getSum().getLongValue());
				Assertions.assertEquals(repeatCount, facetStats.getDocCount());
				Assertions.assertEquals(repeatCount, facetStats.getAllDocCount());
				Assertions.assertEquals(repeatCount, facetStats.getValueCount());
			}
			else if (facetStats.getFacet().equals("top3")) {
				Assertions.assertEquals(4L, facetStats.getMin().getLongValue());
				Assertions.assertEquals(5L, facetStats.getMax().getLongValue());
				Assertions.assertEquals(9L * repeatCount, facetStats.getSum().getLongValue());
				Assertions.assertEquals(2L * repeatCount, facetStats.getDocCount());
				Assertions.assertEquals(2L * repeatCount, facetStats.getAllDocCount());
				Assertions.assertEquals(2L * repeatCount, facetStats.getValueCount());
			}
			else {
				throw new AssertionFailedError("Unexpected facet " + facetStats.getFacet());
			}
		}
	}

	private void authorCountNormalFacetTest(SearchResult searchResult) {
		List<FacetStats> authorCountByNormalFacet = searchResult.getFacetFieldStat("authorCount", "normalFacet");
		for (FacetStats facetStats : authorCountByNormalFacet) {
			if (facetStats.getFacet().equals("foo")) {
				Assertions.assertEquals(2L, facetStats.getMin().getLongValue());
				Assertions.assertEquals(4L, facetStats.getMax().getLongValue());
				Assertions.assertEquals(9L * repeatCount, facetStats.getSum().getLongValue());
				Assertions.assertEquals(3L * repeatCount, facetStats.getDocCount());
				Assertions.assertEquals(3L * repeatCount, facetStats.getAllDocCount());
				Assertions.assertEquals(3L * repeatCount, facetStats.getValueCount());
			}
			else if (facetStats.getFacet().equals("bar")) {
				Assertions.assertEquals(2L, facetStats.getMin().getLongValue());
				Assertions.assertEquals(5L, facetStats.getMax().getLongValue());
				Assertions.assertEquals(7L * repeatCount, facetStats.getSum().getLongValue());
				Assertions.assertEquals(2L * repeatCount, facetStats.getDocCount());
				Assertions.assertEquals(2L * repeatCount, facetStats.getAllDocCount());
				Assertions.assertEquals(2L * repeatCount, facetStats.getValueCount());
			}
			else {
				throw new AssertionFailedError("Unexpected facet " + facetStats.getFacet());
			}
		}
	}

	private void authorCountTest(SearchResult searchResult) {
		FacetStats authorCountStats = searchResult.getNumericFieldStat("authorCount");
		Assertions.assertEquals(2, authorCountStats.getMin().getLongValue());
		Assertions.assertEquals(5, authorCountStats.getMax().getLongValue());

		Assertions.assertEquals(20L * repeatCount, authorCountStats.getSum().getLongValue());
		Assertions.assertEquals(6L * repeatCount, authorCountStats.getDocCount());
		Assertions.assertEquals(6L * repeatCount, authorCountStats.getValueCount());
	}

	@Test
	@Order(8)
	public void testErrorBounds() throws Exception {
		testErrorBoundsSearch(null);
		testErrorBoundsSearch(2);
	}

	private void testErrorBoundsSearch(Integer concurrency) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		Search search = new Search(STAT_TEST_INDEX);
		if (concurrency != null) {
			search.setConcurrency(concurrency);
		}

		// Test 1: With topNShard=-1 (all facets) - no errors should occur
		search.addStat(new StatFacet("authorCount", "pathFacet").setTopN(10).setTopNShard(-1));
		SearchResult searchResult = zuliaWorkPool.search(search);

		List<FacetStats> statsByPathFacet = searchResult.getFacetFieldStat("authorCount", "pathFacet");
		Assertions.assertFalse(statsByPathFacet.isEmpty(), "Should have facet stats");
		for (FacetStats facetStats : statsByPathFacet) {
			Assertions.assertFalse(facetStats.getHasError(),
					"Facet " + facetStats.getFacet() + " should not have error when topNShard=-1");
		}

		// Test 2: With limited topNShard, verify error handling
		// Use topNShard=1 which forces only the top facet per shard
		// pathFacet has 4 values (top1, top2, top3, top4) across 5 shards
		search.clearStat();
		search.addStat(new StatFacet("authorCount", "pathFacet").setTopN(10).setTopNShard(1));
		searchResult = zuliaWorkPool.search(search);

		statsByPathFacet = searchResult.getFacetFieldStat("authorCount", "pathFacet");

		// With topNShard=1, we get the union of top-1 facets from each shard
		// The returned facets that are present in all shards won't have errors
		// Facets missing from some shards will have hasError=true with a valid maxSumError
		for (FacetStats facetStats : statsByPathFacet) {
			if (facetStats.getHasError()) {
				// When hasError is true, maxSumError should be set
				Assertions.assertTrue(facetStats.hasMaxSumError(),
						"Facet " + facetStats.getFacet() + " has error but no maxSumError set");
				// maxSumError represents the potential additional sum from missing shards
				Assertions.assertTrue(facetStats.getMaxSumError().getLongValue() >= 0,
						"maxSumError should be non-negative for facet " + facetStats.getFacet());
			}
		}

		// Test 3: With topNShard=2, more facets returned - verify error bounds are reasonable
		search.clearStat();
		search.addStat(new StatFacet("authorCount", "pathFacet").setTopN(10).setTopNShard(2));
		searchResult = zuliaWorkPool.search(search);

		statsByPathFacet = searchResult.getFacetFieldStat("authorCount", "pathFacet");
		boolean foundAnyFacet = false;
		for (FacetStats facetStats : statsByPathFacet) {
			foundAnyFacet = true;
			if (facetStats.getHasError()) {
				long errorBound = facetStats.getMaxSumError().getLongValue();
				long sum = facetStats.getSum().getLongValue();
				// Error bound is the sum of minimum values from missing shards
				// It should be a reasonable value (not larger than total possible sum)
				Assertions.assertTrue(errorBound >= 0,
						"Error bound should be non-negative for facet " + facetStats.getFacet());
				Assertions.assertTrue(errorBound <= sum + (long) repeatCount * 5 * shardCount,
						"Error bound " + errorBound + " seems unreasonably large for facet " + facetStats.getFacet());
			}
		}
		Assertions.assertTrue(foundAnyFacet, "Should have at least one facet result");

		// Test 4: Verify consistency - same facet with/without limited topNShard should have same sum
		// (the sum should be the same, error bound just indicates potential undercount)
		search.clearStat();
		search.addStat(new StatFacet("authorCount", "normalFacet").setTopN(10).setTopNShard(-1));
		searchResult = zuliaWorkPool.search(search);
		List<FacetStats> allFacetsStats = searchResult.getFacetFieldStat("authorCount", "normalFacet");

		search.clearStat();
		search.addStat(new StatFacet("authorCount", "normalFacet").setTopN(10).setTopNShard(1));
		searchResult = zuliaWorkPool.search(search);
		List<FacetStats> limitedFacetsStats = searchResult.getFacetFieldStat("authorCount", "normalFacet");

		// Find a facet that appears in both results
		for (FacetStats limited : limitedFacetsStats) {
			for (FacetStats all : allFacetsStats) {
				if (limited.getFacet().equals(all.getFacet())) {
					// The sum from limited should be <= sum from all (may be missing some)
					// But if hasError is false, they should be equal
					if (!limited.getHasError()) {
						Assertions.assertEquals(all.getSum().getLongValue(), limited.getSum().getLongValue(),
								"Facet " + limited.getFacet() + " sum should match when present in all shards");
					}
				}
			}
		}
	}

}
