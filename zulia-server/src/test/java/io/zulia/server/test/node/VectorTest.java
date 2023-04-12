package io.zulia.server.test.node;

import com.google.common.primitives.Floats;
import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.VectorTopNQuery;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
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
public class VectorTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(3);

	public static final String VECTOR_TEST = "vectorTest";

	private static final int uniqueDocs = 7;

	@Test
	@Order(1)
	public void createIndex() throws Exception {

		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("description").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createVector("v").index());
		indexConfig.setIndexName(VECTOR_TEST);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20); //force some commits

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void index() throws Exception {

		indexRecord(1, "something special", "red and blue", new float[] { 1.0f, 0, 0, 0 });
		indexRecord(2, "something really special", "reddish and blueish", new float[] { 0.57735027f, 0.57735027f, 0.57735027f, 0 });
		indexRecord(3, "something even more special", "pink with big big big stripes", new float[] { 0f, 1.0f, 0, 0 });
		indexRecord(4, "something special", "real big", new float[] { 0.70710678f, 0.70710678f, 0, 0 });
		indexRecord(5, "something really special", "small", new float[] { 0, 0, 0.70710678f, 0.70710678f });
		indexRecord(6, "something really special", "light-blue with flowers", new float[] { 0.5f, 0.5f, 0.5f, 0.5f });
		indexRecord(7, "boring and small", "plain white and red", null);

	}

	private void indexRecord(int id, String title, String description, float[] vector) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		String uniqueId = String.valueOf(id);

		Document mongoDocument = new Document();
		mongoDocument.put("id", uniqueId);
		mongoDocument.put("title", title);
		mongoDocument.put("description", description);
		mongoDocument.put("v", vector != null ? Floats.asList(vector) : null);

		Store s = new Store(uniqueId, VECTOR_TEST);

		ResultDocBuilder resultDocumentBuilder = ResultDocBuilder.newBuilder().setDocument(mongoDocument);
		s.setResultDocument(resultDocumentBuilder);
		zuliaWorkPool.store(s);

	}

	@Test
	@Order(3)
	public void searchTest() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		Search search = new Search(VECTOR_TEST);
		SearchResult searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(uniqueDocs, searchResult.getTotalHits());

		search = new Search(VECTOR_TEST);
		search.addQuery(new VectorTopNQuery(new float[] { 1.0f, 0.0f, 0.0f, 0.0f }, 3, "v"));
		search.setDebug(true);
		search.setAmount(10);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3, searchResult.getTotalHits());

		Assertions.assertEquals("1", searchResult.getCompleteResults().get(0).getUniqueId());
		Assertions.assertEquals("4", searchResult.getCompleteResults().get(1).getUniqueId());
		Assertions.assertEquals("2", searchResult.getCompleteResults().get(2).getUniqueId());

		search = new Search(VECTOR_TEST);
		search.addQuery(new VectorTopNQuery(new float[] { 1.0f, 0.0f, 0.0f, 0.0f }, 3, "v"));
		search.addQuery(new FilterQuery("red").addQueryField("description"));
		search.setDebug(true);
		search.setAmount(10);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, searchResult.getTotalHits());
		Assertions.assertEquals("1", searchResult.getCompleteResults().get(0).getUniqueId());

		search = new Search(VECTOR_TEST);
		search.addQuery(
				new VectorTopNQuery(new float[] { 1.0f, 0.0f, 0.0f, 0.0f }, 3, "v").addPreFilterQuery(new FilterQuery("blue").addQueryField("description")));
		search.setDebug(true);
		search.setAmount(10);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(2, searchResult.getTotalHits());
		Assertions.assertEquals("1", searchResult.getCompleteResults().get(0).getUniqueId());
		Assertions.assertEquals("6", searchResult.getCompleteResults().get(1).getUniqueId());

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
