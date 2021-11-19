package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.NumericStat;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.StatFacet;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
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
		indexConfig.addFieldConfig(FieldConfigBuilder.create("id", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("title", FieldType.STRING).indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("pathFacet", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD).facetHierarchical().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("normalFacet", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort());
		//indexConfig.addFieldConfig(FieldConfigBuilder.create("authorCount", FieldType.NUMERIC_INT).index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("rating", FieldType.NUMERIC_DOUBLE).index().sort());
		indexConfig.setIndexName(STAT_TEST_INDEX);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20); //force some commits

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void index() throws Exception {

		for (int i = 0; i < repeatCount; i++) {
			indexRecord(i * 5, "something special", "top1/middle/bottom1", "foo", 3, List.of(3.5, 1.0));
			indexRecord(i * 5 + 1, "something really special", "top1/middle/bottom2", "foo", 4, List.of(2.5));
			indexRecord(i * 5 + 2, "something special", "top2/middle/bottom3", "bar", 2, List.of(0.5));
			indexRecord(i * 5 + 3, "something really special", "top3/middle/bottom4", "bar", 5, List.of(3.0));
			indexRecord(i * 5 + 4, "something really special", "top3/middle/bottom4", null, 4, List.of());
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

		ResultDocBuilder resultDocumentBuilder = ResultDocBuilder.newBuilder().setDocument(mongoDocument);
		s.setResultDocument(resultDocumentBuilder);
		zuliaWorkPool.store(s);

	}

	@Test
	@Order(3)
	public void statTest() throws Exception {

		Search search = new Search(STAT_TEST_INDEX);
		search.addStat(new NumericStat("rating"));

		SearchResult searchResult = zuliaWorkPool.search(search);

		FacetStats ratingStat = searchResult.getNumericFieldStat("rating");

		Assertions.assertEquals(0.5, ratingStat.getMin().getDoubleValue(), 0.001);
		Assertions.assertEquals(3.5, ratingStat.getMax().getDoubleValue(), 0.001);

		Assertions.assertEquals(10.5 * repeatCount, ratingStat.getSum().getDoubleValue(), 0.001);
		Assertions.assertEquals(4L * repeatCount, ratingStat.getDocCount());
		Assertions.assertEquals(5L * repeatCount, ratingStat.getValueCount());

		search.clearStat();
		search.addStat(new StatFacet("rating", "normalFacet"));
		searchResult = zuliaWorkPool.search(search);

		List<FacetStats> ratingByFacet = searchResult.getFacetFieldStat("rating", "normalFacet");

		for (FacetStats facetStats : ratingByFacet) {
			if (facetStats.getFacet().equals("foo")) {
				Assertions.assertEquals(1, facetStats.getMin().getDoubleValue(), 0.001);
				Assertions.assertEquals(3.5, facetStats.getMax().getDoubleValue(), 0.001);
				Assertions.assertEquals(7L * repeatCount, facetStats.getSum().getDoubleValue(), 0.001);
				Assertions.assertEquals(2L * repeatCount, facetStats.getDocCount());
				Assertions.assertEquals(3L * repeatCount, facetStats.getValueCount());
			}
			else if (facetStats.getFacet().equals("bar")) {
				Assertions.assertEquals(0.5, facetStats.getMin().getDoubleValue(), 0.001);
				Assertions.assertEquals(3.0, facetStats.getMax().getDoubleValue(), 0.001);
				Assertions.assertEquals(3.5 * repeatCount, facetStats.getSum().getDoubleValue(), 0.001);
				Assertions.assertEquals(2L * repeatCount, facetStats.getDocCount());
				Assertions.assertEquals(2L * repeatCount, facetStats.getValueCount());
			}
			else {
				throw new AssertionFailedError("Unexpect facet <" + facetStats.getFacet() + ">");
			}
		}

		search.clearStat();
		search.addStat(new StatFacet("rating", "pathFacet"));
		searchResult = zuliaWorkPool.search(search);

		List<FacetStats> ratingByPathFacet = searchResult.getFacetFieldStat("rating", "pathFacet");

		for (FacetStats facetStats : ratingByPathFacet) {
			if (facetStats.getFacet().equals("top1")) {
				Assertions.assertEquals(1, facetStats.getMin().getDoubleValue(), 0.001);
				Assertions.assertEquals(3.5, facetStats.getMax().getDoubleValue(), 0.001);
				Assertions.assertEquals(7L * repeatCount, facetStats.getSum().getDoubleValue(), 0.001);
				Assertions.assertEquals(2L * repeatCount, facetStats.getDocCount());
				Assertions.assertEquals(3L * repeatCount, facetStats.getValueCount());
			}
			else if (facetStats.getFacet().equals("top2")) {
				Assertions.assertEquals(0.5, facetStats.getMin().getDoubleValue(), 0.001);
				Assertions.assertEquals(0.5, facetStats.getMax().getDoubleValue(), 0.001);
				Assertions.assertEquals(0.5 * repeatCount, facetStats.getSum().getDoubleValue(), 0.001);
				Assertions.assertEquals(repeatCount, facetStats.getDocCount());
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

		search = new Search(STAT_TEST_INDEX);
		search.addStat(new StatFacet("authorCount", "pathFacet"));
		Search finalSearch = search;
		Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(finalSearch),
				"Expecting: Search: Numeric field <authorCount> must be indexed as a SORTABLE numeric field");

	}

	@Test
	@Order(4)
	public void reindex() throws Exception {
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.create("id", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("title", FieldType.STRING).indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("pathFacet", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD).facetHierarchical().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("normalFacet", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("authorCount", FieldType.NUMERIC_INT).index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("rating", FieldType.NUMERIC_DOUBLE).index().sort());
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
		search.addStat(new NumericStat("authorCount"));

		SearchResult searchResult = zuliaWorkPool.search(search);

		FacetStats ratingStat = searchResult.getNumericFieldStat("authorCount");

		Assertions.assertEquals(2, ratingStat.getMin().getLongValue());
		Assertions.assertEquals(5, ratingStat.getMax().getLongValue());

		Assertions.assertEquals(18L * repeatCount, ratingStat.getSum().getLongValue());
		Assertions.assertEquals(5L * repeatCount, ratingStat.getDocCount());
		Assertions.assertEquals(5L * repeatCount, ratingStat.getValueCount());

		search.clearStat();
		search.addStat(new StatFacet("authorCount", "normalFacet"));
		searchResult = zuliaWorkPool.search(search);

		List<FacetStats> ratingByFacet = searchResult.getFacetFieldStat("authorCount", "normalFacet");

		for (FacetStats facetStats : ratingByFacet) {
			if (facetStats.getFacet().equals("foo")) {
				Assertions.assertEquals(3L, facetStats.getMin().getLongValue());
				Assertions.assertEquals(4L, facetStats.getMax().getLongValue());
				Assertions.assertEquals(7L * repeatCount, facetStats.getSum().getLongValue());
				Assertions.assertEquals(2L * repeatCount, facetStats.getDocCount());
				Assertions.assertEquals(2L * repeatCount, facetStats.getValueCount());
			}
			else if (facetStats.getFacet().equals("bar")) {
				Assertions.assertEquals(2L, facetStats.getMin().getLongValue());
				Assertions.assertEquals(5L, facetStats.getMax().getLongValue());
				Assertions.assertEquals(7L * repeatCount, facetStats.getSum().getLongValue());
				Assertions.assertEquals(2L * repeatCount, facetStats.getDocCount());
				Assertions.assertEquals(2L * repeatCount, facetStats.getValueCount());
			}
			else {
				throw new AssertionFailedError("Unexpect facet <" + facetStats.getFacet() + ">");
			}
		}

		search.clearStat();
		search.addStat(new StatFacet("authorCount", "pathFacet"));
		searchResult = zuliaWorkPool.search(search);

		List<FacetStats> ratingByPathFacet = searchResult.getFacetFieldStat("authorCount", "pathFacet");

		for (FacetStats facetStats : ratingByPathFacet) {
			if (facetStats.getFacet().equals("top1")) {
				Assertions.assertEquals(3L, facetStats.getMin().getLongValue());
				Assertions.assertEquals(4L, facetStats.getMax().getLongValue());
				Assertions.assertEquals(7L * repeatCount, facetStats.getSum().getLongValue());
				Assertions.assertEquals(2L * repeatCount, facetStats.getDocCount());
				Assertions.assertEquals(2L * repeatCount, facetStats.getValueCount());
			}
			else if (facetStats.getFacet().equals("top2")) {
				Assertions.assertEquals(2L, facetStats.getMin().getLongValue());
				Assertions.assertEquals(2L, facetStats.getMax().getLongValue());
				Assertions.assertEquals(2L * repeatCount, facetStats.getSum().getLongValue());
				Assertions.assertEquals(repeatCount, facetStats.getDocCount());
				Assertions.assertEquals(repeatCount, facetStats.getValueCount());
			}
			else if (facetStats.getFacet().equals("top3")) {
				Assertions.assertEquals(4L, facetStats.getMin().getLongValue());
				Assertions.assertEquals(5L, facetStats.getMax().getLongValue());
				Assertions.assertEquals(9L * repeatCount, facetStats.getSum().getLongValue());
				Assertions.assertEquals(2L * repeatCount, facetStats.getDocCount());
				Assertions.assertEquals(2L * repeatCount, facetStats.getValueCount());
			}
			else {
				throw new AssertionFailedError("Unexpect facet <" + facetStats.getFacet() + ">");
			}
		}
	}

	@Test
	@Order(7)
	public void shutdown() throws Exception {
		TestHelper.stopNodes();
		zuliaWorkPool.shutdown();
	}
}
