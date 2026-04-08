package io.zulia.ai.embedding;

import io.zulia.util.VectorUtil;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TextEmbeddingModelTest {

	@ParameterizedTest
	@EnumSource(KnownEmbeddingModel.class)
	void testEmbed(KnownEmbeddingModel knownModel) throws Exception {
		try (TextEmbeddingModel model = TextEmbeddingModel.load(knownModel)) {
			float[] embedding = model.embed("lung cancer immunotherapy");
			assertNotNull(embedding);
			assertEquals(knownModel.getConfig().outputDimensions(), embedding.length);
		}
	}

	@ParameterizedTest
	@EnumSource(KnownEmbeddingModel.class)
	void testEmbedQueryAndPassage(KnownEmbeddingModel knownModel) throws Exception {
		try (TextEmbeddingModel model = TextEmbeddingModel.load(knownModel)) {
			float[] queryEmbedding = model.embedQuery("lung cancer immunotherapy");
			assertNotNull(queryEmbedding);
			assertEquals(model.getDimensions(), queryEmbedding.length);

			float[] passageEmbedding = model.embedPassage("This phase III trial evaluates pembrolizumab in NSCLC patients.");
			assertNotNull(passageEmbedding);
			assertEquals(model.getDimensions(), passageEmbedding.length);
		}
	}

	@ParameterizedTest
	@EnumSource(KnownEmbeddingModel.class)
	void testBatchEmbed(KnownEmbeddingModel knownModel) throws Exception {
		try (TextEmbeddingModel model = TextEmbeddingModel.load(knownModel)) {
			List<String> texts = List.of("lung cancer", "breast cancer", "heart disease");
			List<float[]> embeddings = model.batchEmbed(texts);
			assertEquals(3, embeddings.size());
			for (float[] embedding : embeddings) {
				assertEquals(model.getDimensions(), embedding.length);
			}
		}
	}

	@ParameterizedTest
	@EnumSource(KnownEmbeddingModel.class)
	void testSimilarity(KnownEmbeddingModel knownModel) throws Exception {
		try (TextEmbeddingModel model = TextEmbeddingModel.load(knownModel)) {
			float[] cancerQuery = model.embedQuery("lung cancer treatment");
			float[] cancerDoc = model.embedPassage("A study on non-small cell lung cancer therapy options.");
			float[] unrelatedDoc = model.embedPassage("The history of Renaissance painting in Florence.");

			float similarScore = VectorUtil.cosineSimilarity(cancerQuery, cancerDoc);
			float dissimilarScore = VectorUtil.cosineSimilarity(cancerQuery, unrelatedDoc);

			assertTrue(similarScore > dissimilarScore,
					"%s: related text should have higher similarity (%f) than unrelated text (%f)".formatted(knownModel, similarScore, dissimilarScore));
		}
	}

	@ParameterizedTest
	@EnumSource(KnownEmbeddingModel.class)
	void testDimensions(KnownEmbeddingModel knownModel) throws Exception {
		try (TextEmbeddingModel model = TextEmbeddingModel.load(knownModel)) {
			assertEquals(knownModel.getConfig().outputDimensions(), model.getDimensions());
		}
	}

}
