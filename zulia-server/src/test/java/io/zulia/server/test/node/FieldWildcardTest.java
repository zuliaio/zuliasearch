package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.FieldMapping;
import io.zulia.client.command.builder.ScoredQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.server.test.node.shared.NodeExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FieldWildcardTest {
	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(3);

	public static final String WILDCARD_JSON_TEST_INDEX = "simpleJsonTest";

	@Test
	@Order(1)
	public void createIndex() throws Exception {

		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("docTitle");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("documentId").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("docType").indexAs(DefaultAnalyzers.LC_KEYWORD).sort().facet());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("docTitle").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("altTitle").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("altTitle2").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("docAuthor").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createBool("isParent").index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("parentDocId").index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("docLanguage").indexAs(DefaultAnalyzers.LC_KEYWORD).sort().facet());

		indexConfig.addFieldMapping(new FieldMapping("title").addMappedFields("altTitle", "docTitle"));
		indexConfig.addFieldMapping(new FieldMapping("title2").addMappedFields("*Title"));
		indexConfig.addFieldMapping(new FieldMapping("altTitle").addMappedFields("altTitle2").includeSelf());

		indexConfig.setIndexName(WILDCARD_JSON_TEST_INDEX);
		indexConfig.setNumberOfShards(1);

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void index() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		String json1 = """				
				{
				    "documentId": "1",
				    "docType": "pdf",
				    "docAuthor": "Java Developer Zone",
				    "docTitle": "Search Blog",
				    "altTitle": "Discover Blog",
				    "altTitle2": "Something else totally",
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
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		//run the first search with realtime to force seeing latest changes without waiting for a commit
		Search search = new Search(WILDCARD_JSON_TEST_INDEX).setRealtime(true);
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
		search.addQuery(new ScoredQuery("title:blog"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(4, searchResult.getTotalHits());

		search = new Search(WILDCARD_JSON_TEST_INDEX);
		search.addQuery(new ScoredQuery("title2:blog"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(4, searchResult.getTotalHits());

		search = new Search(WILDCARD_JSON_TEST_INDEX);
		search.addQuery(new ScoredQuery("*:slovak"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, searchResult.getTotalHits());

		search = new Search(WILDCARD_JSON_TEST_INDEX);
		search.addQuery(new ScoredQuery("altTitle2:something"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, searchResult.getTotalHits());

		search = new Search(WILDCARD_JSON_TEST_INDEX);
		search.addQuery(new ScoredQuery("altTitle:something"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, searchResult.getTotalHits());

		search = new Search(WILDCARD_JSON_TEST_INDEX);
		search.addQuery(new ScoredQuery("altTitle:(something OR solr)"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(2, searchResult.getTotalHits());
	}

	@Test
	@Order(4)
	public void internalFieldsExcludedFromWildcardExpansion() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// every stored document lists docTitle in the internal fields-list field, so expanding * into
		// that field would match all 4 documents even though no field's content contains "doctitle"
		Search search = new Search(WILDCARD_JSON_TEST_INDEX);
		search.addQuery(new ScoredQuery("*:docTitle"));
		SearchResult searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(0, searchResult.getTotalHits());

		// every document has 2-element docLanguage lists and 2-character language codes, so expanding *
		// into the list-length and char-length wrap fields would match all 4 documents. Only documentId
		// "2" legitimately contains the term.
		search = new Search(WILDCARD_JSON_TEST_INDEX);
		search.addQuery(new ScoredQuery("*:2"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, searchResult.getTotalHits());

		// every document was just indexed, so expanding * into the internal timestamp field would match
		// all 4 documents for the current year
		search = new Search(WILDCARD_JSON_TEST_INDEX);
		search.addQuery(new ScoredQuery("*:" + java.time.Year.now().getValue()));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(0, searchResult.getTotalHits());

		// explicit references to internal fields are not expansion and must keep working
		search = new Search(WILDCARD_JSON_TEST_INDEX);
		search.addQuery(new ScoredQuery("zuliaId:1"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, searchResult.getTotalHits());

		// explicit char-length query: only "Search Blog" is 11 characters
		search = new Search(WILDCARD_JSON_TEST_INDEX);
		search.addQuery(new ScoredQuery("|docTitle|:11"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, searchResult.getTotalHits());

		// explicit list-length query: every document has a 2-element docLanguage list
		search = new Search(WILDCARD_JSON_TEST_INDEX);
		search.addQuery(new ScoredQuery("|||docLanguage|||:2"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(4, searchResult.getTotalHits());

		// string length wildcard patterns expand against the length-wrap fields: |*Title| covers
		// |docTitle| and |altTitle|, and both "Search Blog" and "Bouncy Blog" are 11 characters
		search = new Search(WILDCARD_JSON_TEST_INDEX);
		search.addQuery(new ScoredQuery("|*Title|:11"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(2, searchResult.getTotalHits());

		// list length wrap pattern: |||docL*||| expands to |||docLanguage|||
		search = new Search(WILDCARD_JSON_TEST_INDEX);
		search.addQuery(new ScoredQuery("|||docL*|||:2"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(4, searchResult.getTotalHits());
	}

}
