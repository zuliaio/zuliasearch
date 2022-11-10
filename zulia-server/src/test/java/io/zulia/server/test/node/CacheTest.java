package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.ScoredQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CacheTest {

	public static final String CACHE_TEST = "cacheTest";

	public static final String CACHE_TEST_2 = "cacheTest2";
	public static final String ALIAS_1 = "alias1";
	public static final String ALIAS_2 = "alias2";

	public static final String ALIAS_3 = "alias3";

	private static ZuliaWorkPool zuliaWorkPool;
	private static final int repeatCount = 50;
	private static final int uniqueDocs = 7;

	@BeforeAll
	public static void initAll() throws Exception {

		TestHelper.createNodes(1);

		TestHelper.startNodes();

		Thread.sleep(2000);

		zuliaWorkPool = TestHelper.createClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("description").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createDouble("rating").index().sort());
		indexConfig.setIndexName(CACHE_TEST);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardQueryCacheSize(3);
		indexConfig.addWarmingSearch(
				new Search(CACHE_TEST).setPinToCache(true).setSearchLabel("important search").addQuery(new FilterQuery("rating:[1.0 TO 3.5]")));

		zuliaWorkPool.createIndex(indexConfig);

		zuliaWorkPool.createIndexAlias(ALIAS_1, CACHE_TEST);
		zuliaWorkPool.createIndexAlias(ALIAS_2, CACHE_TEST);

		indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.setIndexName(CACHE_TEST_2);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardQueryCacheSize(3);
		zuliaWorkPool.createIndex(indexConfig);

		zuliaWorkPool.createIndexAlias(ALIAS_3, CACHE_TEST_2);
	}

	@Test
	@Order(2)
	public void index() throws Exception {

		for (int i = 0; i < repeatCount; i++) {

			indexRecord(CACHE_TEST, i * uniqueDocs, "something special", "red and blue", 1.0);
			indexRecord(CACHE_TEST, i * uniqueDocs + 1, "something really special", "reddish and blueish", 2.4);
			indexRecord(CACHE_TEST, i * uniqueDocs + 2, "something even more special", "pink with big big big stripes", 5.0);
			indexRecord(CACHE_TEST, i * uniqueDocs + 3, "something special", "real big", 4.3);
			indexRecord(CACHE_TEST, i * uniqueDocs + 4, "something really special", "small", 1.6);
			indexRecord(CACHE_TEST, i * uniqueDocs + 5, "something really special", "light-blue with flowers", 4.1);
			indexRecord(CACHE_TEST, i * uniqueDocs + 6, "boring and small", "plain white and red", null);
		}

		indexRecord(CACHE_TEST_2, 12345, "pink and blue", "so many colors", 3.1);
		indexRecord(CACHE_TEST_2, 54321, "purple, red, and blue", "even more colors", 5.4);

	}

	private void indexRecord(String index, int id, String title, String description, Double rating) throws Exception {

		String uniqueId = "" + id;

		Document mongoDocument = new Document();
		mongoDocument.put("id", uniqueId);
		mongoDocument.put("title", title);
		mongoDocument.put("description", description);
		mongoDocument.put("rating", rating);

		Store s = new Store(uniqueId, index);

		ResultDocBuilder resultDocumentBuilder = ResultDocBuilder.newBuilder().setDocument(mongoDocument);
		s.setResultDocument(resultDocumentBuilder);
		zuliaWorkPool.store(s);

	}

	@Test
	@Order(3)
	public void searchTest() throws Exception {

		Thread.sleep(2000);

		Search search;
		SearchResult searchResult;

		search = new Search(CACHE_TEST);
		search.addQuery(new FilterQuery("rating:[1.0 TO 3.5]"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertTrue(searchResult.getFullyCached());
		Assertions.assertEquals(1, searchResult.getShardsCached());
		Assertions.assertEquals(1, searchResult.getShardsPinned());
		Assertions.assertEquals(1, searchResult.getShardsQueried());

		search = new Search(CACHE_TEST);
		search.addQuery(new ScoredQuery("rating:[1.0 TO 2.0]"));
		search.setPinToCache(true);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount * 2, searchResult.getTotalHits());

		search = new Search(CACHE_TEST);
		search.addQuery(new ScoredQuery("rating:[4.0 TO *]"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount * 3, searchResult.getTotalHits());

		search = new Search(CACHE_TEST);
		search.addQuery(new ScoredQuery("blue").addQueryField("title"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(0, searchResult.getTotalHits());

		search = new Search(CACHE_TEST);
		search.addQuery(new ScoredQuery("blue").addQueryField("description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(2 * repeatCount, searchResult.getTotalHits());

		search = new Search(CACHE_TEST);
		search.addQuery(new ScoredQuery("blue").addQueryFields("title", "description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(2 * repeatCount, searchResult.getTotalHits());

		search = new Search(CACHE_TEST);
		search.addQuery(new ScoredQuery("blue").addQueryFields("title", "description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertTrue(searchResult.getFullyCached());

		search = new Search(CACHE_TEST);
		search.addQuery(new ScoredQuery("blue").addQueryField("description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertTrue(searchResult.getFullyCached());

		search = new Search(CACHE_TEST);
		search.addQuery(new ScoredQuery("blue").addQueryField("title"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertTrue(searchResult.getFullyCached());

		search = new Search(CACHE_TEST);
		search.addQuery(new ScoredQuery("rating:[4.0 TO *]"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertFalse(searchResult.getFullyCached());

		search = new Search(CACHE_TEST);
		search.addQuery(new ScoredQuery("rating:[4.0 TO *] AND title:blue"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertFalse(searchResult.getFullyCached());

		search = new Search(CACHE_TEST);
		search.addQuery(new ScoredQuery("rating:[1.0 TO 2.0]"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertTrue(searchResult.getFullyCached());

		indexRecord(CACHE_TEST, 1000, "an amazing title", "pink and purple", 1000.0);

		search = new Search(CACHE_TEST);
		search.addQuery(new ScoredQuery("rating:[4.0 TO *] AND title:blue"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertFalse(searchResult.getFullyCached());

		search = new Search(CACHE_TEST);
		search.addQuery(new ScoredQuery("rating:[1.0 TO 2.0]"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertFalse(searchResult.getFullyCached());

		search = new Search(ALIAS_1);
		search.addQuery(new ScoredQuery("rating:[1.0 TO 2.0]"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertTrue(searchResult.getFullyCached());

		search = new Search(ALIAS_1);
		search.addQuery(new ScoredQuery("title:pink"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertFalse(searchResult.getFullyCached());

		search = new Search(ALIAS_2);
		search.addQuery(new ScoredQuery("title:pink"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertTrue(searchResult.getFullyCached());

		search = new Search(ALIAS_2, ALIAS_3);
		search.addQuery(new ScoredQuery("title:pink"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertFalse(searchResult.getFullyCached());
		Assertions.assertEquals(1, searchResult.getShardsCached());

		search = new Search(CACHE_TEST_2);
		search.addQuery(new ScoredQuery("title:pink"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertTrue(searchResult.getFullyCached());
		Assertions.assertEquals(1, searchResult.getShardsCached());

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
		Search search;
		SearchResult searchResult;

		search = new Search(CACHE_TEST);
		search.addQuery(new FilterQuery("rating:[1.0 TO 3.5]"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertTrue(searchResult.getFullyCached());
		Assertions.assertEquals(1, searchResult.getShardsCached());
		Assertions.assertEquals(1, searchResult.getShardsPinned());
		Assertions.assertEquals(1, searchResult.getShardsQueried());
	}

	@AfterAll
	public static void shutdown() throws Exception {
		TestHelper.stopNodes();
		zuliaWorkPool.shutdown();
	}
}

