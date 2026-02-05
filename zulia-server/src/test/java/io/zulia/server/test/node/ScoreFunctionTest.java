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
import io.zulia.server.test.node.shared.NodeExtension;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;

import java.util.Date;
import java.util.List;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ScoreFunctionTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(1);

	private static final String INDEX_NAME = "scoreFunctionTest";

	@Test
	@Order(1)
	public void createIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("popularity").index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createLong("viewCount").index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createFloat("rating").index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createDouble("boost").index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createDouble("pageRank").index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createDate("publishDate").index().sort());
		indexConfig.setIndexName(INDEX_NAME);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20);

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void indexDocuments() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		long now = System.currentTimeMillis();

		// Document with "cats" - low popularity, low boost
		indexDocument(zuliaWorkPool, "1", "cats are cute", 1, 10L, 1.5f, 1.0, 1.0, new Date(now - 86400000L * 30));

		// Document with "cats" - high popularity, high boost
		indexDocument(zuliaWorkPool, "2", "cats are amazing pets", 100, 1000L, 4.8f, 10.0, 100.0, new Date(now - 86400000L));

		// Document with "cats" - medium popularity
		indexDocument(zuliaWorkPool, "3", "cats love to play", 50, 500L, 3.2f, 5.0, 10.0, new Date(now - 86400000L * 7));

		// Document without "cats" - high popularity
		indexDocument(zuliaWorkPool, "4", "dogs are loyal", 200, 2000L, 4.9f, 20.0, 1000.0, new Date(now));
	}

	private void indexDocument(ZuliaWorkPool zuliaWorkPool, String id, String title, int popularity, long viewCount, float rating, double boost,
			double pageRank, Date publishDate) throws Exception {
		Document doc = new Document();
		doc.put("id", id);
		doc.put("title", title);
		doc.put("popularity", popularity);
		doc.put("viewCount", viewCount);
		doc.put("rating", rating);
		doc.put("boost", boost);
		doc.put("pageRank", pageRank);
		doc.put("publishDate", publishDate);

		zuliaWorkPool.store(new Store(id, INDEX_NAME, ResultDocBuilder.from(doc)));
	}

	@Test
	@Order(3)
	public void testScoreFunctionWithScoreOnly() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Test basic score function with just zuliaScore
		Search search = new Search(INDEX_NAME).setAmount(10).setRealtime(true);
		search.addQuery(new ScoredQuery("title:cats").setScoreFunction("zuliaScore * 2"));
		SearchResult result = zuliaWorkPool.search(search);

		Assertions.assertEquals(3, result.getTotalHits());
	}

	@Test
	@Order(4)
	public void testScoreFunctionWithPopularity() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Without score function - results ordered by text relevance
		Search search = new Search(INDEX_NAME).setAmount(10);
		search.addQuery(new ScoredQuery("title:cats"));
		SearchResult result = zuliaWorkPool.search(search);

		Assertions.assertEquals(3, result.getTotalHits());

		// With score function boosting by popularity - doc 2 (popularity=100) should be first
		search = new Search(INDEX_NAME).setAmount(10);
		search.addQuery(new ScoredQuery("title:cats").setScoreFunction("zuliaScore * popularity"));
		result = zuliaWorkPool.search(search);

		Assertions.assertEquals(3, result.getTotalHits());
		Assertions.assertEquals("2", result.getFirstDocument().get("id"));
	}

	@Test
	@Order(5)
	public void testScoreFunctionWithBoostField() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Boost by the boost field - doc 2 has boost=10.0
		Search search = new Search(INDEX_NAME).setAmount(10);
		search.addQuery(new ScoredQuery("title:cats").setScoreFunction("zuliaScore * boost"));
		SearchResult result = zuliaWorkPool.search(search);

		Assertions.assertEquals(3, result.getTotalHits());
		Assertions.assertEquals("2", result.getFirstDocument().get("id"));
	}

	@Test
	@Order(6)
	public void testScoreFunctionWithLogPageRank() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Use log function with pageRank - doc 2 has pageRank=100
		Search search = new Search(INDEX_NAME).setAmount(10);
		search.addQuery(new ScoredQuery("title:cats").setScoreFunction("zuliaScore * (1 + ln(pageRank))"));
		SearchResult result = zuliaWorkPool.search(search);

		Assertions.assertEquals(3, result.getTotalHits());
		// Doc 2 has highest pageRank (100), so ln(100) ~ 4.6, should rank first
		Assertions.assertEquals("2", result.getFirstDocument().get("id"));
	}

	@Test
	@Order(7)
	public void testScoreFunctionWithMaxFunction() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Use max function to ensure boost is at least 2
		Search search = new Search(INDEX_NAME).setAmount(10);
		search.addQuery(new ScoredQuery("title:cats").setScoreFunction("zuliaScore * max(boost, 2)"));
		SearchResult result = zuliaWorkPool.search(search);

		Assertions.assertEquals(3, result.getTotalHits());
		// Doc 2 has boost=10.0, so gets highest score
		Assertions.assertEquals("2", result.getFirstDocument().get("id"));
	}

	@Test
	@Order(8)
	public void testScoreFunctionWithLongField() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Boost by viewCount (long field) - doc 2 has viewCount=1000
		Search search = new Search(INDEX_NAME).setAmount(10);
		search.addQuery(new ScoredQuery("title:cats").setScoreFunction("zuliaScore * viewCount"));
		SearchResult result = zuliaWorkPool.search(search);

		Assertions.assertEquals(3, result.getTotalHits());
		// Doc 2 has highest viewCount (1000)
		Assertions.assertEquals("2", result.getFirstDocument().get("id"));
	}

	@Test
	@Order(9)
	public void testScoreFunctionWithFloatField() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Boost by rating (float field) - doc 2 has rating=4.8
		Search search = new Search(INDEX_NAME).setAmount(10);
		search.addQuery(new ScoredQuery("title:cats").setScoreFunction("zuliaScore * rating"));
		SearchResult result = zuliaWorkPool.search(search);

		Assertions.assertEquals(3, result.getTotalHits());
		// Doc 2 has highest rating (4.8)
		Assertions.assertEquals("2", result.getFirstDocument().get("id"));
	}

	@Test
	@Order(10)
	public void testScoreFunctionWithDateField() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Use publishDate (date field stored as long epoch millis) in a score function
		// Date values are epoch millis (~1.7 trillion), so use division to create meaningful score differences
		// Doc 2 has the most recent date (largest epoch ms), so dividing by publishDate gives it the smallest divisor
		// Instead, use a recency boost: higher publishDate = more recent = higher score
		Search search = new Search(INDEX_NAME).setAmount(10);
		search.addQuery(new ScoredQuery("title:cats").setScoreFunction("publishDate"));
		SearchResult result = zuliaWorkPool.search(search);

		Assertions.assertEquals(3, result.getTotalHits());
		// Doc 2 has the most recent publish date (highest epoch millis), so it ranks first when scored purely by date
		Assertions.assertEquals("2", result.getFirstDocument().get("id"));
	}

	@Test
	@Order(11)
	public void testScoreFunctionMultiValueUsesMinimum() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Index a document with multi-valued popularity field [200, 5, 150]
		// The SortedNumericSelector.Type.MIN should pick 5 as the value used in scoring
		Document doc = new Document();
		doc.put("id", "5");
		doc.put("title", "cats with multiple values");
		doc.put("popularity", List.of(200, 5, 150));
		doc.put("viewCount", 100L);
		doc.put("rating", 1.0f);
		doc.put("boost", 1.0);
		doc.put("pageRank", 1.0);
		doc.put("publishDate", new Date());
		zuliaWorkPool.store(new Store("5", INDEX_NAME, ResultDocBuilder.from(doc)));

		// Score purely by popularity (no zuliaScore) to eliminate text relevance differences
		// Doc 2: popularity=100, Doc 5: popularity=min(200,5,150)=5
		Search search = new Search(INDEX_NAME).setAmount(10).setRealtime(true);
		search.addQuery(new ScoredQuery("title:cats").setScoreFunction("popularity"));
		SearchResult result = zuliaWorkPool.search(search);

		Assertions.assertEquals(4, result.getTotalHits());

		// Doc 2 (popularity=100) should rank first since MIN of doc 5 is only 5
		Assertions.assertEquals("2", result.getFirstDocument().get("id"));

		// Doc 5 should rank last since MIN(200, 5, 150) = 5 is less than doc 1 (popularity=1)... no, 5 > 1
		// Order should be: doc 2 (100), doc 3 (50), doc 5 (5), doc 1 (1)
		List<Document> docs = result.getDocuments();
		Assertions.assertEquals("2", docs.get(0).get("id"));
		Assertions.assertEquals("3", docs.get(1).get("id"));
		Assertions.assertEquals("5", docs.get(2).get("id"));
		Assertions.assertEquals("1", docs.get(3).get("id"));
	}

	@Test
	@Order(12)
	public void testScoreFunctionWithShouldQuery() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Test score function with a SHOULD query (must=false) combined with match all
		// The SHOULD clause boosts matching docs but doesn't require a match
		Search search = new Search(INDEX_NAME).setAmount(10);
		search.addQuery(new ScoredQuery("*:*"));
		search.addQuery(new ScoredQuery("title:cats", false).setScoreFunction("zuliaScore * popularity"));
		SearchResult result = zuliaWorkPool.search(search);

		// Should return all docs since we have *:* as MUST
		Assertions.assertEquals(5, result.getTotalHits());
		// Doc 2 should rank highest because it matches "cats" and has high popularity
		Assertions.assertEquals("2", result.getFirstDocument().get("id"));
	}

	@Test
	@Order(13)
	public void testScoreFunctionWithComplexExpression() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Complex expression combining multiple fields using sqrt and addition
		Search search = new Search(INDEX_NAME).setAmount(10);
		search.addQuery(new ScoredQuery("title:cats").setScoreFunction("zuliaScore * (sqrt(popularity) + pageRank)"));
		SearchResult result = zuliaWorkPool.search(search);

		Assertions.assertEquals(4, result.getTotalHits());
		// Doc 2: sqrt(100) + 100 = 110
		// Doc 3: sqrt(50) + 10 = 17.07
		// Doc 5: sqrt(min(200,5,150)=5) + 1 = 3.24
		// Doc 1: sqrt(1) + 1 = 2
		Assertions.assertEquals("2", result.getFirstDocument().get("id"));
	}

	@Test
	@Order(14)
	public void testScoreFunctionInvalidField() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Reference a non-existent field should throw an error
		Search search = new Search(INDEX_NAME).setAmount(10);
		search.addQuery(new ScoredQuery("title:cats").setScoreFunction("zuliaScore * nonExistentField"));

		Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(search));
	}

	@Test
	@Order(15)
	public void testScoreFunctionReorderResults() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// First search without score function to establish baseline order
		Search search = new Search(INDEX_NAME).setAmount(10);
		search.addQuery(new ScoredQuery("title:cats"));
		zuliaWorkPool.search(search);

		// Now search with score function that inverts the ranking (favor low popularity)
		search = new Search(INDEX_NAME).setAmount(10);
		search.addQuery(new ScoredQuery("title:cats").setScoreFunction("zuliaScore / (1 + popularity)"));
		SearchResult invertedResult = zuliaWorkPool.search(search);

		// Doc 1 has popularity=1, so it gets highest score after division
		Assertions.assertEquals("1", invertedResult.getFirstDocument().get("id"));
	}

}
