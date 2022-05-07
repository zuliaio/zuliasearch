package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.ScoredQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaQuery.Query.Operator;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SimpleTest {

	public static final String SIMPLE_TEST_INDEX = "simpleTest";

	private static ZuliaWorkPool zuliaWorkPool;
	private static final int repeatCount = 50;
	private static final int uniqueDocs = 7;

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
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("description").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createDouble("rating").index().sort());
		indexConfig.setIndexName(SIMPLE_TEST_INDEX);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20); //force some commits

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void index() throws Exception {

		for (int i = 0; i < repeatCount; i++) {

			indexRecord(i * uniqueDocs, "something special", "red and blue", 1.0);
			indexRecord(i * uniqueDocs + 1, "something really special", "reddish and blueish", 2.4);
			indexRecord(i * uniqueDocs + 2, "something even more special", "pink with big stripes", 5.0);
			indexRecord(i * uniqueDocs + 3, "something special", "real big", 4.3);
			indexRecord(i * uniqueDocs + 4, "something really special", "small", 1.6);
			indexRecord(i * uniqueDocs + 5, "something really special", "light-blue with flowers", 4.1);
			indexRecord(i * uniqueDocs + 6, "boring and small", "plain white and red", null);
		}

	}

	private void indexRecord(int id, String title, String description, Double rating) throws Exception {

		String uniqueId = "" + id;

		Document mongoDocument = new Document();
		mongoDocument.put("id", uniqueId);
		mongoDocument.put("title", title);
		mongoDocument.put("description", description);
		mongoDocument.put("rating", rating);

		Store s = new Store(uniqueId, SIMPLE_TEST_INDEX);

		ResultDocBuilder resultDocumentBuilder = ResultDocBuilder.newBuilder().setDocument(mongoDocument);
		s.setResultDocument(resultDocumentBuilder);
		zuliaWorkPool.store(s);

	}

	@Test
	@Order(3)
	public void searchTest() throws Exception {

		Search search = new Search(SIMPLE_TEST_INDEX);
		SearchResult searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount * uniqueDocs, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("rating:[4.0 TO *]"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount * 3, searchResult.getTotalHits());


		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("blue").addQueryField("title"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(0, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("blue").addQueryField("description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(2*repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("blue").addQueryFields("title", "description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(2*repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("Blue*").addQueryFields("title"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(0, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("Blue*").addQueryFields("description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3*repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("Blue*").addQueryFields("title", "description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3*repeatCount, searchResult.getTotalHits());


		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("small").addQueryFields("title"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("small").addQueryFields("description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("small").addQueryFields("title", "description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(2*repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("small").addQueryFields("title"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("red* BLUE*").addQueryFields("title","description").setDefaultOperator(Operator.AND));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(2*repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("red* BLUE*").addQueryFields("title","description").setDefaultOperator(Operator.OR));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(4*repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("red* BLUE* small").addQueryFields("title","description").setDefaultOperator(Operator.OR).setMinShouldMatch(2));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3*repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("title,description:(red* BLUE* small)").setDefaultOperator(Operator.OR).setMinShouldMatch(2));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3*repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("title,description:(red* BLUE* small)~2").setDefaultOperator(Operator.OR));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3*repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("(red* BLUE* small)~2").addQueryFields("title","description").setDefaultOperator(Operator.OR));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3*repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("\"something really special\"").addQueryFields("title","description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3*repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("\"something really special\" AND rating:[4.0 TO *]").addQueryFields("title","description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3*repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("description:\"something really special\"").addQueryFields("title","description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(0, searchResult.getTotalHits());


		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("boring AND (reddish OR red)").addQueryFields("title","description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("\"something really special\"~1").addQueryFields("title","description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3*repeatCount, searchResult.getTotalHits());


		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("\"something special\"~1").addQueryFields("title","description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(5*repeatCount, searchResult.getTotalHits());

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
		searchTest();
	}

	@Test
	@Order(7)
	public void shutdown() throws Exception {
		TestHelper.stopNodes();
		zuliaWorkPool.shutdown();
	}
}
