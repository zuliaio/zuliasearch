package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.NumericStat;
import io.zulia.client.command.builder.Search;
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

import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class StatTest {

	public static final String STAT_TEST_INDEX = "stat";

	private static ZuliaWorkPool zuliaWorkPool;

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
		indexRecord(1, "something special", "top/middle/bottom1", "value1", 3, List.of(3.5, 1.0));
		indexRecord(2, "something really special", "top/middle/bottom2", "value2", 4, List.of(2.5));
		indexRecord(3, "something special", "top/middle/bottom3", "value3", 3, List.of(0.5));
		indexRecord(4, "something really special", "top/middle/bottom4", "value4", 4, List.of(3.0));
		indexRecord(5, "something really special", "top/middle/bottom4", "value4", 4, List.of());

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
		Assertions.assertEquals(0.5, ratingStat.getMin().getDoubleValue(), 0.001);
		Assertions.assertEquals(10.5, ratingStat.getSum().getDoubleValue(), 0.001);
		Assertions.assertEquals(4, ratingStat.getDocCount());
		Assertions.assertEquals(5, ratingStat.getValueCount());

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
		//indexConfig.addFieldConfig(FieldConfigBuilder.create("rating", FieldType.NUMERIC_DOUBLE).index().sort());
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

	}

	@Test
	@Order(7)
	public void shutdown() throws Exception {
		TestHelper.stopNodes();
		zuliaWorkPool.shutdown();
	}
}
