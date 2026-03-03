package io.zulia.server.test.node;

import com.google.common.primitives.Floats;
import io.zulia.DefaultAnalyzers;
import io.zulia.ai.embedding.KnownEmbeddingModel;
import io.zulia.ai.embedding.TextEmbeddingModel;
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
public class EmbeddingVectorTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(1);

	private static final String INDEX_NAME = "embeddingVectorTest";

	private static TextEmbeddingModel embeddingModel;

	private static final String[] TITLES = { "Phase III trial of pembrolizumab in non-small cell lung cancer",
			"Immunotherapy response rates in advanced melanoma patients", "CRISPR-Cas9 gene editing for sickle cell disease treatment",
			"Cardiovascular outcomes with SGLT2 inhibitors in type 2 diabetes", "Deep learning for early detection of diabetic retinopathy",
			"Renaissance art and its influence on modern architecture", "Soil microbiome diversity in tropical rainforests",
			"Quantum computing approaches to protein folding simulation" };

	@Test
	@Order(1)
	public void loadModel() throws Exception {
		embeddingModel = TextEmbeddingModel.load(KnownEmbeddingModel.E5_SMALL_V2);
	}

	@Test
	@Order(2)
	public void createIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD));
		indexConfig.addFieldConfig(FieldConfigBuilder.createVector("embedding").index());
		indexConfig.setIndexName(INDEX_NAME);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20);

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(3)
	public void indexDocuments() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		for (int i = 0; i < TITLES.length; i++) {
			String title = TITLES[i];
			float[] embedding = embeddingModel.embedPassage(title);

			Document mongoDocument = new Document();
			mongoDocument.put("title", title);
			mongoDocument.put("embedding", Floats.asList(embedding));

			Store store = new Store(String.valueOf(i), INDEX_NAME);
			store.setResultDocument(ResultDocBuilder.newBuilder().setDocument(mongoDocument));
			zuliaWorkPool.store(store);
		}
	}

	@Test
	@Order(4)
	public void vectorSearch() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// search for lung cancer related documents
		float[] queryVec = embeddingModel.embedQuery("lung cancer immunotherapy treatment");

		Search search = new Search(INDEX_NAME).setRealtime(true);
		search.addQuery(new VectorTopNQuery(queryVec, 3, "embedding"));
		search.setAmount(10);

		SearchResult searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3, searchResult.getTotalHits());

		// top result should be the lung cancer paper
		String topTitle = searchResult.getFirstDocument().getString("title");
		Assertions.assertTrue(topTitle.contains("lung cancer"), "Top result should be lung cancer paper, got: " + topTitle);
	}

	@Test
	@Order(5)
	public void vectorSearchWithTextFilter() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// broad medical query but filtered to diabetes-related text
		float[] queryVec = embeddingModel.embedQuery("medical treatment outcomes");

		Search search = new Search(INDEX_NAME).setRealtime(true);
		search.addQuery(new VectorTopNQuery(queryVec, 5, "embedding"));
		search.addQuery(new FilterQuery("diabet*").addQueryField("title"));
		search.setAmount(10);

		SearchResult searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(2, searchResult.getTotalHits());
	}

	@Test
	@Order(6)
	public void vectorSearchWithPreFilter() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// vector search pre-filtered to cancer documents
		float[] queryVec = embeddingModel.embedQuery("clinical trial results");

		Search search = new Search(INDEX_NAME).setRealtime(true);
		search.addQuery(new VectorTopNQuery(queryVec, 3, "embedding").addPreFilterQuery(new FilterQuery("cancer OR melanoma").addQueryField("title")));
		search.setAmount(10);

		SearchResult searchResult = zuliaWorkPool.search(search);

		// only lung cancer and melanoma docs should match the pre-filter
		Assertions.assertEquals(2, searchResult.getTotalHits());
	}

	@Test
	@Order(7)
	public void closeModel() {
		if (embeddingModel != null) {
			embeddingModel.close();
		}
	}

}
