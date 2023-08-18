package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.ZuliaConstants;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.Sort;
import io.zulia.client.command.builder.TermQuery;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaIndex;
import io.zulia.server.test.node.shared.NodeExtension;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TermQueryTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(3);

	public static final String TERM_QUERY_TEST = "tqTest";

	private static final int UNIQUE_DOCS = 9;

	private static final int REPEATS = 40;

	@Test
	@Order(1)
	public void createIndex() throws Exception {

		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(
				FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.KEYWORD, "id").sortAs(ZuliaIndex.SortAs.StringHandling.LOWERCASE_FOLDING, "id"));
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("field1").indexAs(DefaultAnalyzers.STANDARD));
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("field2").indexAs(DefaultAnalyzers.LC_KEYWORD));

		indexConfig.setIndexName(TERM_QUERY_TEST);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20); //force some commits

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void index() throws Exception {

		for (int i = 0; i < REPEATS; i++) {
			indexRecord(1 + (i * UNIQUE_DOCS), "abc", "DEF");
			indexRecord(2 + (i * UNIQUE_DOCS), "def", "aaa");
			indexRecord(3 + (i * UNIQUE_DOCS), "rrr abc", "sss");
			indexRecord(4 + (i * UNIQUE_DOCS), "abc", "rqs");
			indexRecord(5 + (i * UNIQUE_DOCS), "rrr", "tuv");
			indexRecord(6 + (i * UNIQUE_DOCS), "wew", "sss");
			indexRecord(7 + (i * UNIQUE_DOCS), "weee", "def");
			indexRecord(8 + (i * UNIQUE_DOCS), "dd", "ss");
			indexRecord(9 + (i * UNIQUE_DOCS), "aa", "qq");
		}

	}

	private void indexRecord(int id, String field1, String field2) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		String uniqueId = String.valueOf(id) + "-i";

		Document mongoDocument = new Document();
		mongoDocument.put("id", uniqueId);
		mongoDocument.put("field1", field1);
		mongoDocument.put("field2", field2);

		Store s = new Store(uniqueId, TERM_QUERY_TEST);

		ResultDocBuilder resultDocumentBuilder = ResultDocBuilder.newBuilder().setDocument(mongoDocument);
		s.setResultDocument(resultDocumentBuilder);
		zuliaWorkPool.store(s);

	}

	@Test
	@Order(3)
	public void searchTest() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		Search search = new Search(TERM_QUERY_TEST);
		SearchResult searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(UNIQUE_DOCS * REPEATS, searchResult.getTotalHits());

		search = new Search(TERM_QUERY_TEST);
		search.addQuery(new TermQuery("field1").addTerms("abc", "def"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(4 * REPEATS, searchResult.getTotalHits());

		search = new Search(TERM_QUERY_TEST);
		search.addQuery(new TermQuery("field2").addTerms("def"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(2 * REPEATS, searchResult.getTotalHits());

		search = new Search(TERM_QUERY_TEST);
		search.addQuery(new TermQuery("field1", "field2").addTerms("abc", "def"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(5 * REPEATS, searchResult.getTotalHits());

		search = new Search(TERM_QUERY_TEST);
		search.addQuery(new TermQuery("field1", "field2").addTerms("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(0, searchResult.getTotalHits());

		search = new Search(TERM_QUERY_TEST);
		search.addQuery(
				new TermQuery("field1", "field2").addTerms("abc", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3 * REPEATS, searchResult.getTotalHits());

		search = new Search(TERM_QUERY_TEST);
		search.addQuery(
				new TermQuery("field1", "field2").addTerms("abc", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16"));
		search.addSort(new Sort(ZuliaConstants.SCORE_FIELD));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3 * REPEATS, searchResult.getTotalHits());

		search = new Search(TERM_QUERY_TEST);
		search.addQuery(new TermQuery("field2").addTerms("def", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16"));
		search.addSort(new Sort(ZuliaConstants.SCORE_FIELD));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(2 * REPEATS, searchResult.getTotalHits());

		search = new Search(TERM_QUERY_TEST);
		search.addQuery(
				new TermQuery("id").addTerms("def", "1-i", "2-i", "3-i", "4-i", "5-i", "6-i", "7-i", "8-i", "9-i", "10-i", "11-i", "12-i", "13-i", "14-i",
						"15-i", "16-i"));
		search.addSort(new Sort(ZuliaConstants.SCORE_FIELD));
		search.setDebug(true);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(16, searchResult.getTotalHits());

		search = new Search(TERM_QUERY_TEST);
		search.addQuery(new TermQuery("id").addTerms("a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a10", "a11", "a12", "a13", "a14", "a15", "16-i"));
		search.addSort(new Sort(ZuliaConstants.SCORE_FIELD));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, searchResult.getTotalHits());

		search = new Search(TERM_QUERY_TEST);
		search.addQuery(
				new TermQuery("zuliaId").addTerms("a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a10", "a11", "a12", "a13", "a14", "a15", "16-i"));
		search.addSort(new Sort(ZuliaConstants.SCORE_FIELD));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, searchResult.getTotalHits());

		search = new Search(TERM_QUERY_TEST);
		search.addQuery(new FilterQuery("""
				id:zl:tq(blah "1-i" "2-i" "3-i" "4-i" "5-i" "6-i" "7-i" "8-i" "9-i" "10-i" "11-i" "12-i" "13-i" "14-i" "15-i" "16-i")
				"""));
		search.addSort(new Sort(ZuliaConstants.SCORE_FIELD));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(16, searchResult.getTotalHits());
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
