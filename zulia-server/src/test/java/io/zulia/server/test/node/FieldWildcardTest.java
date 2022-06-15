package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.ScoredQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.fields.FieldConfigBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FieldWildcardTest {

	public static final String WILDCARD_JSON_TEST_INDEX = "simpleJsonTest";

	private static ZuliaWorkPool zuliaWorkPool;

	@BeforeAll
	public static void initAll() throws Exception {

		TestHelper.createNodes(3);

		TestHelper.startNodes();

		Thread.sleep(2000);

		zuliaWorkPool = TestHelper.createClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("docTitle");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("documentId").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("docType").indexAs(DefaultAnalyzers.LC_KEYWORD).sort().facet());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("docTitle").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("altTitle").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("docAuthor").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createBool("isParent").index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("parentDocId").index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("docLanguage").indexAs(DefaultAnalyzers.LC_KEYWORD).sort().facet());

		indexConfig.setIndexName(WILDCARD_JSON_TEST_INDEX);
		indexConfig.setNumberOfShards(1);

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void index() throws Exception {

		String json1 = """				
				{
				    "documentId": "1",
				    "docType": "pdf",
				    "docAuthor": "Java Developer Zone",
				    "docTitle": "Search Blog",
				    "altTitle": "Discover Blog",
				    "isParent": true,
				    "docLanguage": [
				      "en",
				      "fr"
				    ]
				}""";
		String json2 = """
				{
				  "documentId": "2",
				  "docType": "pdf",
				  "docAuthor": "Java Developer Zone",
				  "docTitle": "Spring Boot Blog",
				  "altTitle": "Bouncy Blog",
				  "isParent": true,
				  "docLanguage": [
				    "en",
				    "fr"
				  ]
				}""";
		String json3 = """
				{
				  "documentId": "3",
				  "docType": "pdf",
				  "docAuthor": "Java Developer Zone",
				  "docTitle": "Solr Blog",
				  "altTitle": "Apache Solr Blog",
				  "isParent": false,
				  "parentDocId": 1,
				  "docLanguage": [
				    "fr",
				    "slovak"
				  ]
				}""";
		String json4 = """
				{
				  "documentId": "4",
				  "docType": "pdf",
				  "docAuthor": "Java Developer Zone",
				  "docTitle": "Elastic Search Blog",
				  "altTitle": "ES Blog",
				  "isParent": false,
				  "parentDocId": 1,
				  "docLanguage": [
				    "en",
				    "czech"
				  ]
				}""";

		zuliaWorkPool.store(new Store("1", WILDCARD_JSON_TEST_INDEX).setResultDocument(json1));
		zuliaWorkPool.store(new Store("2", WILDCARD_JSON_TEST_INDEX).setResultDocument(json2));
		zuliaWorkPool.store(new Store("3", WILDCARD_JSON_TEST_INDEX).setResultDocument(json3));
		zuliaWorkPool.store(new Store("4", WILDCARD_JSON_TEST_INDEX).setResultDocument(json4));

	}

	@Test
	@Order(3)
	public void searchTest() throws Exception {

		Search search = new Search(WILDCARD_JSON_TEST_INDEX);
		SearchResult searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(4, searchResult.getTotalHits());

		search = new Search(WILDCARD_JSON_TEST_INDEX);
		search.addQuery(new ScoredQuery("*Title:apache"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, searchResult.getTotalHits());

		search = new Search(WILDCARD_JSON_TEST_INDEX);
		search.addQuery(new ScoredQuery("*Title:spring"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, searchResult.getTotalHits());

		search = new Search(WILDCARD_JSON_TEST_INDEX);
		search.addQuery(new ScoredQuery("*Title:blog"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(4, searchResult.getTotalHits());


		search = new Search(WILDCARD_JSON_TEST_INDEX);
		search.addQuery(new ScoredQuery("*:slovak"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, searchResult.getTotalHits());
	}

	@Test
	@Order(7)
	public void shutdown() throws Exception {
		TestHelper.stopNodes();
		zuliaWorkPool.shutdown();
	}
}
