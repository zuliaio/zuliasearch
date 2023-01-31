package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.Sort;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import org.bson.Document;
import org.junit.jupiter.api.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FieldChangeTest {

    private static ZuliaWorkPool zuliaWorkPool;

    private static final String INDEX_NAME = "fieldChange";

    @BeforeAll
    public static void initAll() throws Exception {

        TestHelper.createNodes(3);

        TestHelper.startNodes();

        Thread.sleep(2000);

        zuliaWorkPool = TestHelper.createClient();
    }

    @Test
    @Order(1)
    public void indexingTest() throws Exception {

        ClientIndexConfig indexConfig = new ClientIndexConfig();
        indexConfig.addDefaultSearchField("title");
        indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
        indexConfig.addFieldConfig(FieldConfigBuilder.createString("field1").indexAs(DefaultAnalyzers.STANDARD).sort().facet());
        indexConfig.addFieldConfig(FieldConfigBuilder.createInt("field2").sort().facet());
        indexConfig.setIndexName(INDEX_NAME);
        indexConfig.setNumberOfShards(1);
        zuliaWorkPool.createIndex(indexConfig);

        {
            String uniqueId = "" + 1;
            Document mongoDocument = new Document();
            mongoDocument.put("id", uniqueId);
            mongoDocument.put("field1", "someValue");
            mongoDocument.put("field2", 123);
            zuliaWorkPool.store(new Store(uniqueId, INDEX_NAME, ResultDocBuilder.from(mongoDocument)));
        }

        indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
        indexConfig.addFieldConfig(FieldConfigBuilder.createInt("field1").sort().facet());
        indexConfig.addFieldConfig(FieldConfigBuilder.createString("field2").indexAs(DefaultAnalyzers.STANDARD).sort().facet());
        indexConfig.setIndexName(INDEX_NAME);
        indexConfig.setNumberOfShards(1);
        zuliaWorkPool.createIndex(indexConfig);

        {

            String uniqueId = "" + 1;
            Document mongoDocument = new Document();
            mongoDocument.put("id", uniqueId);
            mongoDocument.put("field1", 4343);
            mongoDocument.put("field2", "important value");
            zuliaWorkPool.store(new Store(uniqueId, INDEX_NAME, ResultDocBuilder.from(mongoDocument)));

            uniqueId = "" + 2;
            mongoDocument = new Document();
            mongoDocument.put("id", uniqueId);
            mongoDocument.put("field1", 555);
            mongoDocument.put("field2", "some value");
            zuliaWorkPool.store(new Store(uniqueId, INDEX_NAME, ResultDocBuilder.from(mongoDocument)));

            uniqueId = "" + 3;
            mongoDocument = new Document();
            mongoDocument.put("id", uniqueId);
            mongoDocument.put("field1", 4);
            mongoDocument.put("field2", "abcd");
            zuliaWorkPool.store(new Store(uniqueId, INDEX_NAME, ResultDocBuilder.from(mongoDocument)));
        }
    }

    @Test
    @Order(2)
    public void sortTestAfterFieldChange() throws Exception {
        SearchResult searchResult;

        Search search = new Search(INDEX_NAME).setAmount(10);

        search.addSort(new Sort("field1"));
        searchResult = zuliaWorkPool.search(search);
        Assertions.assertEquals(4, searchResult.getFirstDocument().get("field1"));

        search.clearSort();
        search.addSort(new Sort("field1").descending());
        searchResult = zuliaWorkPool.search(search);
        Assertions.assertEquals(4343, searchResult.getFirstDocument().get("field1"));

        search.clearSort();
        search.addSort(new Sort("field2"));
        searchResult = zuliaWorkPool.search(search);
        Assertions.assertEquals("abcd", searchResult.getFirstDocument().get("field2"));

        search.clearSort();
        search.addSort(new Sort("field2").descending());
        searchResult = zuliaWorkPool.search(search);
        Assertions.assertEquals("some value", searchResult.getFirstDocument().get("field2"));

    }

    @AfterAll
    public static void shutdown() throws Exception {
        TestHelper.stopNodes();
        zuliaWorkPool.shutdown();
    }
}
