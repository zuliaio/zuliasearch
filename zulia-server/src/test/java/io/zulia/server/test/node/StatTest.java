package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.MatchAllQuery;
import io.zulia.client.command.builder.NumericStat;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.StatFacet;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaQuery.FacetStats;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.opentest4j.AssertionFailedError;

import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class StatTest {

	public static final String STAT_TEST_INDEX = "stat";

	private static ZuliaWorkPool zuliaWorkPool;
	private static int repeatCount = 100;

	@BeforeAll
	public static void initAll() throws Exception {

		TestHelper.createNodes(3);

		TestHelper.startNodes();

		Thread.sleep(2000);

		zuliaWorkPool = TestHelper.createClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("pathFacet").indexAs(DefaultAnalyzers.LC_KEYWORD).facetHierarchical().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("normalFacet").indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort());
		//indexConfig.addFieldConfig(FieldConfigBuilder.create("authorCount", FieldType.NUMERIC_INT).index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createDouble("rating").index().sort());
		indexConfig.setIndexName(STAT_TEST_INDEX);
		indexConfig.setNumberOfShards(1);
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
			indexRecord(i * uniqueDocs + 6, "boring", "top4/middle/bottom5", "other", 1, List.of());
		}

	}

	private void indexRecord(int id, String title, String pathFacet, String normalFacet, int authorCount, List<Double> rating) throws Exception {

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

		Search search = new Search(STAT_TEST_INDEX);
		search.addStat(new NumericStat("rating"));
		search.addQuery(new FilterQuery("title:boring").exclude());
		search.addQuery(new MatchAllQuery());

		SearchResult searchResult = zuliaWorkPool.search(search);
		System.out.println("Hits: " + searchResult.getTotalHits());

		FacetStats ratingStat = searchResult.getNumericFieldStat("rating");
		ratingTest(ratingStat);

		search.clearStat();
		search.addStat(new StatFacet("rating", "normalFacet"));
		searchResult = zuliaWorkPool.search(search);
		ratingNormalTest(searchResult);

		search.clearStat();
		search.addStat(new StatFacet("rating", "pathFacet"));
		searchResult = zuliaWorkPool.search(search);

		ratingPathTest(searchResult);

		search.clearStat();
		search.addStat(new StatFacet("rating", "normalFacet"));
		search.addStat(new StatFacet("rating", "pathFacet"));
		searchResult = zuliaWorkPool.search(search);
		ratingNormalTest(searchResult);
		ratingPathTest(searchResult);

		search = new Search(STAT_TEST_INDEX);
		search.addStat(new StatFacet("authorCount", "pathFacet"));
		Search finalSearch = search;
		Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(finalSearch),
				"Expecting: Search: Numeric field <authorCount> must be indexed as a SORTABLE numeric field");

		search = new Search(STAT_TEST_INDEX);
		search.addStat(new StatFacet("rating", "normalFacet"));
		search.addQuery(new FilterQuery("title:boring"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());
		System.out.println(searchResult.getFacetFieldStats());
		List<FacetStats> ratingByFacet = searchResult.getFacetFieldStat("rating", "normalFacet");
		Assertions.assertEquals(1, ratingByFacet.size());
	}

	private void ratingTest(FacetStats ratingStat) {
		Assertions.assertEquals(0.5, ratingStat.getMin().getDoubleValue(), 0.001);
		Assertions.assertEquals(3.5, ratingStat.getMax().getDoubleValue(), 0.001);

		Assertions.assertEquals(10.5 * repeatCount, ratingStat.getSum().getDoubleValue(), 0.001);
		Assertions.assertEquals(4L * repeatCount, ratingStat.getDocCount());
		Assertions.assertEquals(6L * repeatCount, ratingStat.getAllDocCount());
		Assertions.assertEquals(5L * repeatCount, ratingStat.getValueCount());
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
	public void reindex() throws Exception {
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("pathFacet").indexAs(DefaultAnalyzers.LC_KEYWORD).facetHierarchical().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("normalFacet").indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("authorCount").index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createDouble("rating").index().sort());
		indexConfig.setIndexName(STAT_TEST_INDEX);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20); //force some commits

		zuliaWorkPool.createIndex(indexConfig);

		//trigger indexing again with path2 added in the index config
		index();

	}

	@Test
	@Order(5)
	public void restart() throws Exception {
		TestHelper.stopNodes();
		Thread.sleep(2000);
		TestHelper.startNodes();
		Thread.sleep(2000);
	}

	@Test
	@Order(6)
	public void confirm() throws Exception {
		Search search = new Search(STAT_TEST_INDEX);
		search.addQuery(new FilterQuery("title:boring").exclude());
		search.addQuery(new MatchAllQuery());
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
				throw new AssertionFailedError("Unexpected facet <" + facetStats.getFacet() + ">");
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
				throw new AssertionFailedError("Unexpected facet <" + facetStats.getFacet() + ">");
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
	@Order(7)
	public void shutdown() throws Exception {
		TestHelper.stopNodes();
		zuliaWorkPool.shutdown();
	}
}
