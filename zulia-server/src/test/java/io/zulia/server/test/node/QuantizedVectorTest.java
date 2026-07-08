package io.zulia.server.test.node;

import com.google.common.primitives.Floats;
import io.zulia.DefaultAnalyzers;
import io.zulia.ai.embedding.KnownEmbeddingModel;
import io.zulia.ai.embedding.TextEmbeddingModel;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.VectorTopNQuery;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.CompleteResult;
import io.zulia.client.result.GetIndexConfigResult;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.fields.VectorIndexingConfigBuilder;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.message.ZuliaIndex.IndexAs;
import io.zulia.message.ZuliaIndex.VectorDescription;
import io.zulia.message.ZuliaIndex.VectorIndexingConfig;
import io.zulia.server.test.node.shared.NodeExtension;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedMap;
import java.util.stream.Collectors;

/**
 * End-to-end coverage of the quantized vector path: every encoding round-trips, oversample rescore recovers exact
 * ordering, dual-representation fields and VectorDescription read-back work, and everything survives a node restart
 * (SPI codec resolution).
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class QuantizedVectorTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(1);

	private static final String INDEX_NAME = "quantizedVectorTest";

	private static TextEmbeddingModel embeddingModel;

	private static final String[] TITLES = { "Phase III trial of pembrolizumab in non-small cell lung cancer",
			"Immunotherapy response rates in advanced melanoma patients", "CRISPR-Cas9 gene editing for sickle cell disease treatment",
			"Cardiovascular outcomes with SGLT2 inhibitors in type 2 diabetes", "Deep learning for early detection of diabetic retinopathy",
			"Renaissance art and its influence on modern architecture", "Soil microbiome diversity in tropical rainforests",
			"Quantum computing approaches to protein folding simulation" };

	// One indexed vector field per encoding. All populated from the same embedding so results are directly comparable.
	private static final SequencedMap<String, VectorIndexingConfig.Encoding> ENCODING_FIELDS = new LinkedHashMap<>();

	static {
		ENCODING_FIELDS.put("vFloat", VectorIndexingConfig.Encoding.FLOAT32);
		ENCODING_FIELDS.put("vInt8", VectorIndexingConfig.Encoding.INT8);
		ENCODING_FIELDS.put("vInt7", VectorIndexingConfig.Encoding.INT7);
		ENCODING_FIELDS.put("vInt4", VectorIndexingConfig.Encoding.INT4);
		ENCODING_FIELDS.put("vBBQ", VectorIndexingConfig.Encoding.BBQ);
		ENCODING_FIELDS.put("vBBQ2", VectorIndexingConfig.Encoding.BBQ_2BIT);
	}

	private static final String FLAT_FIELD = "vFlat";       // INT8, exact brute-force
	private static final String EUCLID_FIELD = "vEuclid";   // FLOAT32, EUCLIDEAN similarity
	private static final String MIP_FIELD = "vMip";         // INT8, MAX_INNER_PRODUCT similarity
	private static final String RAW_FIELD = "vRaw";         // no vector config at all, inherits the index-version default (INT8 for this new index)
	private static final String DIMS_FIELD = "vDims";       // INT8 with declared dimensions (length validated on store)

	// one stored field indexed under two names with different encodings, the vector is supplied once per document
	private static final String DUAL_FIELD = "vDual";
	private static final String DUAL_EXACT = "vDualExact";  // FLOAT32 representation
	private static final String DUAL_BBQ = "vDualBBQ";      // BBQ representation
	private static final String DUAL_MODEL_NAME = "intfloat/e5-small-v2";

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

		for (Map.Entry<String, VectorIndexingConfig.Encoding> entry : ENCODING_FIELDS.entrySet()) {
			indexConfig.addFieldConfig(FieldConfigBuilder.createVector(entry.getKey()).quantization(entry.getValue()).index());
		}

		indexConfig.addFieldConfig(FieldConfigBuilder.createVector(FLAT_FIELD).quantization(VectorIndexingConfig.Encoding.INT8).flat().index());
		indexConfig.addFieldConfig(FieldConfigBuilder.createVector(EUCLID_FIELD).quantization(VectorIndexingConfig.Encoding.FLOAT32)
				.similarity(VectorDescription.Similarity.EUCLIDEAN).index());
		indexConfig.addFieldConfig(FieldConfigBuilder.createVector(MIP_FIELD).quantization(VectorIndexingConfig.Encoding.INT8)
				.similarity(VectorDescription.Similarity.MAX_INNER_PRODUCT).index());
		indexConfig.addFieldConfig(
				FieldConfigBuilder.createVector(DIMS_FIELD).quantization(VectorIndexingConfig.Encoding.INT8).dimensions(embeddingModel.getDimensions()).index());

		indexConfig.addFieldConfig(FieldConfigBuilder.createVector(DUAL_FIELD).dimensions(embeddingModel.getDimensions())
				.model(DUAL_MODEL_NAME, "test embedding model").indexAs(DUAL_EXACT, VectorIndexingConfig.Encoding.FLOAT32)
				.indexAs(DUAL_BBQ, VectorIndexingConfigBuilder.create().quantization(VectorIndexingConfig.Encoding.BBQ).hnsw(24, 200)));

		// no vector config at all, so it inherits the index-creation-version default (INT8 for this new index)
		indexConfig.addFieldConfig(FieldConfig.newBuilder().setStoredFieldName(RAW_FIELD).setFieldType(FieldConfig.FieldType.VECTOR)
				.addIndexAs(IndexAs.newBuilder().setIndexFieldName(RAW_FIELD).build()).build());

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
			List<Float> embedding = Floats.asList(embeddingModel.embedPassage(TITLES[i]));

			Document mongoDocument = new Document();
			mongoDocument.put("title", TITLES[i]);
			for (String field : ENCODING_FIELDS.keySet()) {
				mongoDocument.put(field, embedding);
			}
			mongoDocument.put(FLAT_FIELD, embedding);
			mongoDocument.put(EUCLID_FIELD, embedding);
			mongoDocument.put(MIP_FIELD, embedding);
			mongoDocument.put(DIMS_FIELD, embedding);
			mongoDocument.put(RAW_FIELD, embedding);
			mongoDocument.put(DUAL_FIELD, embedding);

			Store store = new Store(String.valueOf(i), INDEX_NAME);
			store.setResultDocument(ResultDocBuilder.newBuilder().setDocument(mongoDocument));
			zuliaWorkPool.store(store);
		}
	}

	@Test
	@Order(4)
	public void everyEncodingIsSearchable() throws Exception {
		// without oversampling 1-bit/2-bit BBQ can shift the rank-1 hit, so lossy encodings only need to return hits.
		// Recall recovery is proven by oversampleRescoreMatchesExact()
		for (Map.Entry<String, VectorIndexingConfig.Encoding> entry : ENCODING_FIELDS.entrySet()) {
			if (isLossyBinary(entry.getValue())) {
				Assertions.assertEquals(3, topIds(entry.getKey(), lungCancerQuery(), 3, 0f).size(), "field " + entry.getKey() + " should return hits");
			}
			else {
				assertTopHitIsLungCancer(entry.getKey());
			}
		}
		assertTopHitIsLungCancer(FLAT_FIELD);
		assertTopHitIsLungCancer(EUCLID_FIELD);
		assertTopHitIsLungCancer(MIP_FIELD);
		assertTopHitIsLungCancer(RAW_FIELD);
		assertTopHitIsLungCancer(DIMS_FIELD);
	}

	private static boolean isLossyBinary(VectorIndexingConfig.Encoding encoding) {
		return encoding == VectorIndexingConfig.Encoding.BBQ || encoding == VectorIndexingConfig.Encoding.BBQ_2BIT;
	}

	private float[] lungCancerQuery() throws Exception {
		return embeddingModel.embedQuery("lung cancer immunotherapy treatment");
	}

	@Test
	@Order(5)
	public void oversampleRescoreMatchesExact() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// with 8 docs and 4x oversample the rescore reranks the whole corpus with exact vectors, so every quantized
		// field must match the FLOAT32 ordering
		String[] queries = { "lung cancer immunotherapy treatment", "gene editing therapy", "diabetes outcomes" };
		for (String queryText : queries) {
			float[] queryVec = embeddingModel.embedQuery(queryText);
			List<String> exactOrder = topIds("vFloat", queryVec, 5, 0f);
			for (VectorIndexingConfig.Encoding encoding : ENCODING_FIELDS.values()) {
				if (encoding == VectorIndexingConfig.Encoding.FLOAT32) {
					continue;
				}
				String field = fieldFor(encoding);
				List<String> rescored = topIds(field, queryVec, 5, 4.0f);
				Assertions.assertEquals(exactOrder, rescored,
						"Oversample rescore on " + field + " should match exact float32 ordering for query: " + queryText);
			}
		}
	}

	@Test
	@Order(6)
	public void bbqDefaultsToRescore() throws Exception {
		// with no oversample set, BBQ rescores by its encoding default, so its top hit matches the exact ordering
		float[] queryVec = lungCancerQuery();
		String exactTop = topIds("vFloat", queryVec, 3, 0f).getFirst();
		for (VectorIndexingConfig.Encoding encoding : List.of(VectorIndexingConfig.Encoding.BBQ, VectorIndexingConfig.Encoding.BBQ_2BIT)) {
			String defaultedTop = topIds(fieldFor(encoding), queryVec, 3, 0f).getFirst();
			Assertions.assertEquals(exactTop, defaultedTop,
					encoding + " with no explicit oversample should rescore by default and recover the exact top hit");
		}
	}

	@Test
	@Order(7)
	public void dualRepresentationsSearchable() throws Exception {
		// each representation searches through its own graph: the FLOAT32 twin exactly, the BBQ twin via rescore
		float[] queryVec = lungCancerQuery();
		List<String> exactOrder = topIds("vFloat", queryVec, 5, 0f);
		Assertions.assertEquals(exactOrder, topIds(DUAL_EXACT, queryVec, 5, 0f), "FLOAT32 representation should match the exact float32 ordering");
		Assertions.assertEquals(exactOrder, topIds(DUAL_BBQ, queryVec, 5, 4.0f), "BBQ representation with oversample should match the exact ordering");
		Assertions.assertEquals(exactOrder.getFirst(), topIds(DUAL_BBQ, queryVec, 3, 0f).getFirst(),
				"BBQ representation should recover the exact top hit through its default rescore");
	}

	@Test
	@Order(8)
	public void vectorDescriptionReadBack() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		GetIndexConfigResult indexConfigResult = zuliaWorkPool.getIndexConfig(INDEX_NAME);
		ClientIndexConfig readBack = indexConfigResult.getIndexConfig();

		FieldConfig dualFieldConfig = readBack.getFieldConfig(DUAL_FIELD);
		Assertions.assertTrue(dualFieldConfig.hasVectorDescription(), "Field " + DUAL_FIELD + " should read back a VectorDescription");
		VectorDescription dualDescription = dualFieldConfig.getVectorDescription();
		Assertions.assertEquals(embeddingModel.getDimensions(), dualDescription.getDimensions());
		Assertions.assertEquals(DUAL_MODEL_NAME, dualDescription.getModelName());
		Assertions.assertEquals("test embedding model", dualDescription.getModelDescription());
		Assertions.assertEquals(VectorDescription.Similarity.DEFAULT, dualDescription.getSimilarity(), "no explicit similarity was set");

		Map<String, VectorIndexingConfig.Encoding> encodingByRepresentation = dualFieldConfig.getIndexAsList().stream()
				.collect(Collectors.toMap(IndexAs::getIndexFieldName, ia -> ia.getVectorIndexingConfig().getEncoding()));
		Assertions.assertEquals(Map.of(DUAL_EXACT, VectorIndexingConfig.Encoding.FLOAT32, DUAL_BBQ, VectorIndexingConfig.Encoding.BBQ),
				encodingByRepresentation, "each representation should read back its own encoding");

		VectorIndexingConfig bbqIndexingConfig = dualFieldConfig.getIndexAsList().stream().filter(ia -> DUAL_BBQ.equals(ia.getIndexFieldName())).findFirst()
				.orElseThrow().getVectorIndexingConfig();
		Assertions.assertEquals(24, bbqIndexingConfig.getHnswM(), "hnsw m from VectorIndexingConfigBuilder should round-trip");
		Assertions.assertEquals(200, bbqIndexingConfig.getHnswEfConstruction(), "hnsw efConstruction from VectorIndexingConfigBuilder should round-trip");

		VectorDescription euclidDescription = readBack.getFieldConfig(EUCLID_FIELD).getVectorDescription();
		Assertions.assertEquals(VectorDescription.Similarity.EUCLIDEAN, euclidDescription.getSimilarity(), "explicit similarity should round-trip");
	}

	@Test
	@Order(9)
	public void invalidOversampleIsRejected() throws Exception {
		// a NaN or negative oversample is a client error and must be rejected, not silently treated as off
		float[] queryVec = lungCancerQuery();
		for (float bad : new float[] { -1.0f, Float.NaN }) {
			Search search = new Search(INDEX_NAME).setRealtime(true);
			VectorTopNQuery vectorQuery = new VectorTopNQuery(queryVec, 3, "vBBQ");
			vectorQuery.oversample(bad);
			search.addQuery(vectorQuery);
			search.setAmount(3);
			Assertions.assertThrows(Exception.class, () -> nodeExtension.getClient().search(search), "Oversample " + bad + " should be rejected");
		}
	}

	@Test
	@Order(10)
	public void dimensionMismatchIsRejected() {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		Document badDocument = new Document();
		badDocument.put("title", "bad vector");
		badDocument.put(DIMS_FIELD, Floats.asList(0.1f, 0.2f, 0.3f)); // wrong length for a declared-dimension field

		Store store = new Store("bad", INDEX_NAME);
		store.setResultDocument(ResultDocBuilder.newBuilder().setDocument(badDocument));

		Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.store(store),
				"Storing a vector whose length disagrees with the declared dimensions should fail");
	}

	@Test
	@Order(11)
	public void restart() throws Exception {
		nodeExtension.restartNodes();
	}

	@Test
	@Order(12)
	public void confirmAfterRestart() throws Exception {
		// reopening the index from disk resolves the "Zulia104" codec by its persisted name via SPI
		everyEncodingIsSearchable();
		oversampleRescoreMatchesExact();
		dualRepresentationsSearchable();
	}

	@Test
	@Order(13)
	public void closeModel() {
		if (embeddingModel != null) {
			embeddingModel.close();
		}
	}

	private static String fieldFor(VectorIndexingConfig.Encoding encoding) {
		return ENCODING_FIELDS.entrySet().stream().filter(e -> e.getValue() == encoding).map(Map.Entry::getKey).findFirst().orElseThrow();
	}

	private void assertTopHitIsLungCancer(String field) throws Exception {
		List<String> ids = topIds(field, lungCancerQuery(), 3, 0f);
		Assertions.assertEquals(3, ids.size(), "Expected 3 hits on field " + field);
		String topTitle = TITLES[Integer.parseInt(ids.getFirst())];
		Assertions.assertTrue(topTitle.contains("lung cancer"), "Top hit on field " + field + " should be the lung cancer paper, got: " + topTitle);
	}

	private List<String> topIds(String field, float[] queryVec, int topN, float oversample) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		Search search = new Search(INDEX_NAME).setRealtime(true);
		VectorTopNQuery vectorQuery = new VectorTopNQuery(queryVec, topN, field);
		if (oversample > 0f) {
			vectorQuery.oversample(oversample);
		}
		search.addQuery(vectorQuery);
		search.setAmount(topN);
		SearchResult searchResult = zuliaWorkPool.search(search);
		return searchResult.getUniqueIds();
	}
}
