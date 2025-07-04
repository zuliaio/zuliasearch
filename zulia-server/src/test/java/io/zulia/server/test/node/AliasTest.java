package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Fetch;
import io.zulia.client.command.GetTerms;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.ScoredQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.FetchResult;
import io.zulia.client.result.GetFieldsResult;
import io.zulia.client.result.GetIndexConfigResult;
import io.zulia.client.result.GetIndexesResult;
import io.zulia.client.result.GetNodesResult;
import io.zulia.client.result.GetNumberOfDocsResult;
import io.zulia.client.result.GetTermsResult;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.server.test.node.shared.NodeExtension;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)

public class AliasTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(3);

	public static final String ALIAS_TEST_INDEX = "aliasTest";

	private static final int repeatCount = 50;
	private static final int uniqueDocs = 7;

	@Test
	@Order(1)
	public void createIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createDouble("rating").index().sort());
		indexConfig.setIndexName(ALIAS_TEST_INDEX);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20); //force some commits

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void aliasIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		for (int i = 0; i < repeatCount; i++) {

			indexRecord(ALIAS_TEST_INDEX, i * uniqueDocs, "something special", 1.0);
			indexRecord(ALIAS_TEST_INDEX, i * uniqueDocs + 1, "something really special", 2.4);
			indexRecord(ALIAS_TEST_INDEX, i * uniqueDocs + 2, "something even more special", 5.0);
			indexRecord(ALIAS_TEST_INDEX, i * uniqueDocs + 3, "something special", 4.3);
			indexRecord(ALIAS_TEST_INDEX, i * uniqueDocs + 4, "something really special", 1.6);
			indexRecord(ALIAS_TEST_INDEX, i * uniqueDocs + 5, "something really special", 4.1);
			indexRecord(ALIAS_TEST_INDEX, i * uniqueDocs + 6, "boring", null);
		}

		zuliaWorkPool.createIndexAlias("someAlias", ALIAS_TEST_INDEX);

		for (int i = repeatCount; i < repeatCount * 2; i++) {

			indexRecord("someAlias", i * uniqueDocs, "something special", 1.0);
			indexRecord("someAlias", i * uniqueDocs + 1, "something really special", 2.4);
			indexRecord("someAlias", i * uniqueDocs + 2, "something even more special", 5.0);
			indexRecord("someAlias", i * uniqueDocs + 3, "something special", 4.3);
			indexRecord("someAlias", i * uniqueDocs + 4, "something really special", 1.6);
			indexRecord("someAlias", i * uniqueDocs + 5, "something really special", 4.1);
			indexRecord("someAlias", i * uniqueDocs + 6, "boring", null);
		}

	}

	private void indexRecord(String index, int id, String title, Double rating) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		String uniqueId = String.valueOf(id);

		Document mongoDocument = new Document();
		mongoDocument.put("id", uniqueId);
		mongoDocument.put("title", title);
		mongoDocument.put("rating", rating);

		Store s = new Store(uniqueId, index);

		ResultDocBuilder resultDocumentBuilder = ResultDocBuilder.newBuilder().setDocument(mongoDocument);
		s.setResultDocument(resultDocumentBuilder);
		zuliaWorkPool.store(s);

	}

	@Test
	@Order(3)
	public void aliasSearchTest() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		Search search = new Search(ALIAS_TEST_INDEX);
		SearchResult searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount * 2 * uniqueDocs, searchResult.getTotalHits());

		search = new Search("someAlias");
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount * 2 * uniqueDocs, searchResult.getTotalHits());

		search = new Search(ALIAS_TEST_INDEX);
		search.addQuery(new ScoredQuery("rating:[4.0 TO *]"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount * 2 * 3, searchResult.getTotalHits());

		search = new Search("someAlias");
		search.addQuery(new ScoredQuery("rating:[4.0 TO *]"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount * 2 * 3, searchResult.getTotalHits());

		GetNumberOfDocsResult numberOfDocs = zuliaWorkPool.getNumberOfDocs("someAlias");
		Assertions.assertEquals(repeatCount * 2 * uniqueDocs, numberOfDocs.getNumberOfDocs());

		GetFieldsResult getFields = zuliaWorkPool.getFields("someAlias");
		Assertions.assertEquals(3, getFields.getFieldNames().size());
		Assertions.assertTrue(getFields.getFieldNames().contains("id"));
		Assertions.assertTrue(getFields.getFieldNames().contains("title"));
		Assertions.assertTrue(getFields.getFieldNames().contains("rating"));

		GetTermsResult getTermsResult = zuliaWorkPool.execute(new GetTerms("someAlias", "title"));
		Assertions.assertEquals(6, getTermsResult.getTerms().size());

		GetIndexesResult indexes = zuliaWorkPool.getIndexes();
		Assertions.assertTrue(indexes.getIndexNames().contains(ALIAS_TEST_INDEX));
		Assertions.assertFalse(indexes.getIndexNames().contains("someAlias"));
		Assertions.assertEquals(1, indexes.getIndexNames().size());

		GetNodesResult nodes = zuliaWorkPool.getNodes();
		Assertions.assertEquals(1, nodes.getIndexAliases().size());

		FetchResult fetch = zuliaWorkPool.fetch(new Fetch("1", "someAlias"));
		Assertions.assertEquals("1", fetch.getDocument().getString("id"));

		GetIndexConfigResult indexConfig = zuliaWorkPool.getIndexConfig("someAlias");
		Assertions.assertEquals(ALIAS_TEST_INDEX, indexConfig.getIndexConfig().getIndexName());

	}

	@Test
	@Order(5)
	public void restart() throws Exception {
		nodeExtension.restartNodes();
	}

	@Test
	@Order(6)
	public void confirm() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		Search search = new Search(ALIAS_TEST_INDEX);
		SearchResult searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount * 2 * uniqueDocs, searchResult.getTotalHits());

		search = new Search("someAlias");
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount * 2 * uniqueDocs, searchResult.getTotalHits());

		Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(new Search("someAlias2")), "fail with index does not exist");
		zuliaWorkPool.createIndexAlias("someAlias2", ALIAS_TEST_INDEX);
		search = new Search("someAlias2");
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount * 2 * uniqueDocs, searchResult.getTotalHits());

		zuliaWorkPool.deleteIndexAlias("someAlias");
		Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(new Search("someAlias")), "fail with index does not exist");

		zuliaWorkPool.createIndexAlias("someAlias2", ALIAS_TEST_INDEX);
		search = new Search("someAlias2");
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount * 2 * uniqueDocs, searchResult.getTotalHits());

		zuliaWorkPool.deleteIndexAlias("someAlias2");
		Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(new Search("someAlias")), "fail with index does not exist");

	}

}
