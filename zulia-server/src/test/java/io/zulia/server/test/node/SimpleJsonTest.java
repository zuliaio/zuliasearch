package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.fields.FieldConfigBuilder;
import org.junit.jupiter.api.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SimpleJsonTest {

    public static final String SIMPLE_JSON_TEST_INDEX = "simpleJsonTest";

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
        indexConfig.addFieldConfig(FieldConfigBuilder.createString("docAuthor").indexAs(DefaultAnalyzers.STANDARD).sort());
        indexConfig.addFieldConfig(FieldConfigBuilder.createBool("isParent").index().sort());
        indexConfig.addFieldConfig(FieldConfigBuilder.createInt("parentDocId").index().sort());
        indexConfig.addFieldConfig(FieldConfigBuilder.createString("docLanguage").indexAs(DefaultAnalyzers.LC_KEYWORD).sort().facet());

        indexConfig.setIndexName(SIMPLE_JSON_TEST_INDEX);
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
                  "isParent": false,
                  "parentDocId": 1,
                  "docLanguage": [
                    "en",
                    "czech"
                  ]
                }""";

        zuliaWorkPool.store(new Store("1", SIMPLE_JSON_TEST_INDEX).setResultDocument(json1));
        zuliaWorkPool.store(new Store("2", SIMPLE_JSON_TEST_INDEX).setResultDocument(json2));
        zuliaWorkPool.store(new Store("3", SIMPLE_JSON_TEST_INDEX).setResultDocument(json3));
        zuliaWorkPool.store(new Store("4", SIMPLE_JSON_TEST_INDEX).setResultDocument(json4));

    }

    @Test
    @Order(3)
    public void searchTest() throws Exception {

        Search search = new Search(SIMPLE_JSON_TEST_INDEX);
        SearchResult searchResult = zuliaWorkPool.search(search);
        Assertions.assertEquals(4, searchResult.getTotalHits());

        search = new Search(SIMPLE_JSON_TEST_INDEX);
        search.addQuery(new FilterQuery("docAuthor:\"Java Developer Zone\""));
        searchResult = zuliaWorkPool.search(search);
        Assertions.assertEquals(4, searchResult.getTotalHits());

        search = new Search(SIMPLE_JSON_TEST_INDEX);
        search.addQuery(new FilterQuery("parentDocId:1"));
        searchResult = zuliaWorkPool.search(search);
        Assertions.assertEquals(2, searchResult.getTotalHits());

        search = new Search(SIMPLE_JSON_TEST_INDEX);
        search.addQuery(new FilterQuery("isParent:true"));
        searchResult = zuliaWorkPool.search(search);
        Assertions.assertEquals(2, searchResult.getTotalHits());

        search = new Search(SIMPLE_JSON_TEST_INDEX);
        search.addCountFacet(new CountFacet("docLanguage").setTopN(10));
        searchResult = zuliaWorkPool.search(search);
        Assertions.assertEquals("en", searchResult.getFacetCounts("docLanguage").get(0).getFacet());
        Assertions.assertEquals(3, searchResult.getFacetCounts("docLanguage").get(0).getCount());

        search = new Search(SIMPLE_JSON_TEST_INDEX).setAmount(1);
        search.addQuery(new FilterQuery("documentId:3"));
        searchResult = zuliaWorkPool.search(search);
        Assertions.assertEquals(1, searchResult.getTotalHits());

        String singleLineJson = "{\"documentId\": \"3\", \"docType\": \"pdf\", \"docAuthor\": \"Java Developer Zone\", \"docTitle\": \"Solr Blog\", \"isParent\": false, \"parentDocId\": 1, \"docLanguage\": [\"fr\", \"slovak\"]}";
        Assertions.assertEquals(singleLineJson, searchResult.getFirstDocumentAsJson());
        Assertions.assertEquals(singleLineJson, searchResult.getDocumentsAsJson().get(0));


        String prettyJson = """
                {
                  "documentId": "3",
                  "docType": "pdf",
                  "docAuthor": "Java Developer Zone",
                  "docTitle": "Solr Blog",
                  "isParent": false,
                  "parentDocId": 1,
                  "docLanguage": [
                    "fr",
                    "slovak"
                  ]
                }""";

        Assertions.assertEquals(prettyJson, searchResult.getFirstDocumentAsPrettyJson());
        Assertions.assertEquals(prettyJson, searchResult.getDocumentsAsPrettyJson().get(0));
    }

    @AfterAll
    public static void shutdown() throws Exception {
        TestHelper.stopNodes();
        zuliaWorkPool.shutdown();
    }
}
