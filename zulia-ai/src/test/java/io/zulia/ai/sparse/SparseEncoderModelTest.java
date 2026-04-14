package io.zulia.ai.sparse;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparseEncoderModelTest {

	@ParameterizedTest
	@EnumSource(KnownSparseModel.class)
	void testEncode(KnownSparseModel knownModel) throws Exception {
		try (var model = SparseEncoderModel.load(knownModel)) {
			Map<String, Float> sparse = model.encode("lung cancer immunotherapy");
			assertNotNull(sparse);
			assertFalse(sparse.isEmpty(), "sparse representation should not be empty");
			sparse.values().forEach(weight -> assertTrue(weight > 0, "all weights should be positive"));
		}
	}

	@ParameterizedTest
	@EnumSource(KnownSparseModel.class)
	void testTermExpansion(KnownSparseModel knownModel) throws Exception {
		try (var model = SparseEncoderModel.load(knownModel)) {
			Map<String, Float> sparse = model.encode("lung cancer immunotherapy");
			assertTrue(sparse.size() > 3, "sparse representation should contain expanded terms beyond the input tokens");
		}
	}

	@ParameterizedTest
	@EnumSource(KnownSparseModel.class)
	void testMaxTermsCapped(KnownSparseModel knownModel) throws Exception {
		var knownConfig = knownModel.getConfig();
		var builder = SparseModelConfig.builder(knownConfig.modelUrl()).maxTerms(50);
		if (knownConfig.includeTokenTypes() != null) {
			builder.includeTokenTypes(knownConfig.includeTokenTypes());
		}
		var config = builder.build();
		try (var model = SparseEncoderModel.load(config)) {
			Map<String, Float> sparse = model.encode("lung cancer immunotherapy treatment options and clinical trial results");
			assertTrue(sparse.size() <= 50, "sparse representation should not exceed maxTerms (50), got %d".formatted(sparse.size()));
		}
	}

	@ParameterizedTest
	@EnumSource(KnownSparseModel.class)
	void testWeightsDescending(KnownSparseModel knownModel) throws Exception {
		try (var model = SparseEncoderModel.load(knownModel)) {
			Map<String, Float> sparse = model.encode("lung cancer immunotherapy");
			float previous = Float.MAX_VALUE;
			for (float weight : sparse.values()) {
				assertTrue(weight <= previous, "weights should be in descending order");
				previous = weight;
			}
		}
	}

	@ParameterizedTest
	@EnumSource(KnownSparseModel.class)
	void testBatchEncode(KnownSparseModel knownModel) throws Exception {
		try (var model = SparseEncoderModel.load(knownModel)) {
			List<String> texts = List.of("lung cancer", "breast cancer", "heart disease");
			List<Map<String, Float>> results = model.batchEncode(texts);
			assertEquals(3, results.size());
			for (Map<String, Float> sparse : results) {
				assertFalse(sparse.isEmpty());
			}
		}
	}

	@ParameterizedTest
	@EnumSource(KnownSparseModel.class)
	void testNoSpecialTokens(KnownSparseModel knownModel) throws Exception {
		try (var model = SparseEncoderModel.load(knownModel)) {
			Map<String, Float> sparse = model.encode("lung cancer immunotherapy");
			for (String token : sparse.keySet()) {
				assertFalse(SparseTranslator.SPECIAL_TOKENS.contains(token),
						"special token should not appear in output: " + token);
			}
		}
	}
}
