package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.ZuliaFieldConstants;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.Highlight;
import io.zulia.client.command.builder.ScoredQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.CompleteResult;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaQuery.Query.Operator;
import io.zulia.server.test.node.shared.NodeExtension;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SimpleTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(3);

	public static final String SIMPLE_TEST_INDEX = "simpleTest";

	private static final int repeatCount = 50;
	private static final int uniqueDocs = 7;

	@Test
	@Order(1)
	public void createIndex() throws Exception {

		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD_HTML).sort());
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
			indexRecord(i * uniqueDocs + 2, "something even more special", "pink with big big big st<i>ri</i>pes", 5.0);
			indexRecord(i * uniqueDocs + 3, "something special", "real big", 4.3);
			indexRecord(i * uniqueDocs + 4, "something really special", "small", 1.6);
			indexRecord(i * uniqueDocs + 5, "something really spe<b>ci</b>al", "light-blue with flowers", 4.1);
			indexRecord(i * uniqueDocs + 6, "boring and small", "plain white and red", null);
		}

	}

	private void indexRecord(int id, String title, String description, Double rating) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		String uniqueId = String.valueOf(id);

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
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		//run the first search with realtime to force seeing latest changes without waiting for a commit
		Search search = new Search(SIMPLE_TEST_INDEX).setRealtime(true);
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
		Assertions.assertEquals(2 * repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("blue").addQueryFields("title", "description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(2 * repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("Blue*").addQueryFields("title"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(0, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("Blue*").addQueryFields("description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3 * repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("Blue*").addQueryFields("title", "description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3 * repeatCount, searchResult.getTotalHits());

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
		Assertions.assertEquals(2 * repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("small").addQueryFields("title"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("red* BLUE*").addQueryFields("title", "description").setDefaultOperator(Operator.AND));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(2 * repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("red* BLUE*").addQueryFields("title", "description").setDefaultOperator(Operator.OR));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(4 * repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("red* BLUE* small").addQueryFields("title", "description").setDefaultOperator(Operator.OR).setMinShouldMatch(2));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3 * repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("title,description:(red* BLUE* small)").setDefaultOperator(Operator.OR).setMinShouldMatch(2));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3 * repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("title,description:(red* BLUE* small)~2").setDefaultOperator(Operator.OR));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3 * repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("title,description:(red* BLUE* small)@2").setDefaultOperator(Operator.OR));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3 * repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("(red* BLUE* small)~2").addQueryFields("title", "description").setDefaultOperator(Operator.OR));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3 * repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("\"something really special\"").addQueryFields("title", "description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3 * repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("\"something really special\" AND rating:[4.0 TO *]").addQueryFields("title", "description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("description:\"something really special\"").addQueryFields("title", "description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(0, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("boring AND (reddish OR red)").addQueryFields("title", "description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("\"something really special\"~1").addQueryFields("title", "description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3 * repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("\"something special\"~1").addQueryFields("title", "description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(5 * repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("fn:ordered(pink big)").addQueryFields("title", "description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("fn:ordered(big pink)").addQueryFields("title", "description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(0, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("fn:ordered(pink big) AND rating:[1 TO 3.0]").addQueryFields("title", "description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(0, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("fn:ordered(pink big) AND rating:[3.0 TO 5.0]").addQueryFields("title", "description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("title:fn:ordered(pink big) AND rating:[3.0 TO 5.0]").addQueryFields("title", "description"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(0, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("description:fn:ordered(pink big) AND rating:[3.0 TO 5.0]").addQueryFields("id", "rating"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("description:big").addQueryFields("id", "rating"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(2 * repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("description:fn:ordered(big big)").addQueryFields("id", "rating"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("description:fn:ordered(big big big)").addQueryFields("id", "rating"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("description:fn:unordered(big big big)").addQueryFields("id", "rating"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("description:fn:ordered(big big big big)").addQueryFields("id", "rating"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(0, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX);
		search.addQuery(new ScoredQuery("description:fn:unordered(big big big big)").addQueryFields("id", "rating"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(0, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX).setAmount(10);
		search.addQuery(new ScoredQuery("white").addQueryFields("description"));
		search.addHighlight(new Highlight("description"));
		searchResult = zuliaWorkPool.search(search);
		CompleteResult firstCompleteResult = searchResult.getFirstCompleteResult();
		List<String> titleHighlightsForFirstDoc = firstCompleteResult.getHighlightsForField("description");
		Assertions.assertEquals(1, titleHighlightsForFirstDoc.size());
		String expected = titleHighlightsForFirstDoc.getFirst();
		Assertions.assertEquals("plain <em>white</em> and red", expected);

		search = new Search(SIMPLE_TEST_INDEX).setAmount(10);
		search.addQuery(new ScoredQuery("white").addQueryFields("description"));
		search.addHighlight(new Highlight("description").setPreTag("<b>").setPostTag("</b>"));
		searchResult = zuliaWorkPool.search(search);
		firstCompleteResult = searchResult.getFirstCompleteResult();
		titleHighlightsForFirstDoc = firstCompleteResult.getHighlightsForField("description");
		Assertions.assertEquals(1, titleHighlightsForFirstDoc.size());
		expected = titleHighlightsForFirstDoc.getFirst();
		Assertions.assertEquals("plain <b>white</b> and red", expected);

		search = new Search(SIMPLE_TEST_INDEX).setAmount(10);
		search.addQuery(new ScoredQuery(ZuliaFieldConstants.TIMESTAMP_FIELD + ":[* TO *]"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount * uniqueDocs, searchResult.getTotalHits());

		String yesterday = LocalDate.now(ZoneOffset.UTC).minusDays(1).toString();
		String tomorrow = LocalDate.now(ZoneOffset.UTC).plusDays(1).toString();

		search = new Search(SIMPLE_TEST_INDEX).setAmount(10);
		search.addQuery(new ScoredQuery(ZuliaFieldConstants.TIMESTAMP_FIELD + ":[" + yesterday + " TO " + tomorrow + "]"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount * uniqueDocs, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX).setAmount(10);
		search.addQuery(new ScoredQuery(ZuliaFieldConstants.TIMESTAMP_FIELD + ":[* TO " + yesterday + "]"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(0, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX).setAmount(10);
		search.addQuery(new ScoredQuery(ZuliaFieldConstants.TIMESTAMP_FIELD + ":[" + tomorrow + " TO *]"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(0, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX).setAmount(10);
		search.addQuery(new ScoredQuery("description:stripes")); //no html cleaning on description
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(0, searchResult.getTotalHits());


		search = new Search(SIMPLE_TEST_INDEX).setAmount(10);
		search.addQuery(new ScoredQuery("description:\"st<i>ri</i>pes\""));  //no HTML cleaning on description
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX).setAmount(10);
		search.addQuery(new ScoredQuery("title:special"));  // HTML cleaning on title lets spe<b>ci</b>al match
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(6 * repeatCount, searchResult.getTotalHits());

		search = new Search(SIMPLE_TEST_INDEX).setAmount(10);
		search.addQuery(new ScoredQuery("title:\"<b>special</b>\""));  // HTML cleaning on title lets HTML from search be stripped even if no match in the text
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(6 * repeatCount, searchResult.getTotalHits());
	}

	@Test
	@Order(5)
	public void restart() throws Exception {
		nodeExtension.restartNodes();
	}

	@Test
	@Order(6)
	public void confirm() throws Exception {
		searchTest();
	}

}
