package io.zulia.ai.sparse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparseEncoderModelTest {

	private static final String MODEL_DIR_ENV = "SPARSE_MODEL_DIR";

	@Test
	@EnabledIfEnvironmentVariable(named = MODEL_DIR_ENV, matches = ".+")
	void testEncode() throws Exception {
		Path modelDir = Path.of(System.getenv(MODEL_DIR_ENV));
		var config = SparseModelConfig.builder("unused").build();
		try (var model = SparseEncoderModel.loadFromDirectory(modelDir, config)) {
			Map<String, Float> sparse = model.encode("lung cancer immunotherapy");
			assertNotNull(sparse);
			assertFalse(sparse.isEmpty(), "sparse representation should not be empty");
			sparse.values().forEach(weight -> assertTrue(weight > 0, "all weights should be positive"));
		}
	}

	@Test
	@EnabledIfEnvironmentVariable(named = MODEL_DIR_ENV, matches = ".+")
	void testTermExpansion() throws Exception {
		Path modelDir = Path.of(System.getenv(MODEL_DIR_ENV));
		var config = SparseModelConfig.builder("unused").build();
		try (var model = SparseEncoderModel.loadFromDirectory(modelDir, config)) {
			Map<String, Float> sparse = model.encode("lung cancer immunotherapy");
			assertTrue(sparse.size() > 3, "sparse representation should contain expanded terms beyond the input tokens");
		}
	}

	@Test
	@EnabledIfEnvironmentVariable(named = MODEL_DIR_ENV, matches = ".+")
	void testMaxTermsCapped() throws Exception {
		Path modelDir = Path.of(System.getenv(MODEL_DIR_ENV));
		var config = SparseModelConfig.builder("unused").maxTerms(50).build();
		try (var model = SparseEncoderModel.loadFromDirectory(modelDir, config)) {
			Map<String, Float> sparse = model.encode("lung cancer immunotherapy treatment options and clinical trial results");
			assertTrue(sparse.size() <= 50, "sparse representation should not exceed maxTerms (50), got %d".formatted(sparse.size()));
		}
	}

	@Test
	@EnabledIfEnvironmentVariable(named = MODEL_DIR_ENV, matches = ".+")
	void testWeightsDescending() throws Exception {
		Path modelDir = Path.of(System.getenv(MODEL_DIR_ENV));
		var config = SparseModelConfig.builder("unused").build();
		try (var model = SparseEncoderModel.loadFromDirectory(modelDir, config)) {
			Map<String, Float> sparse = model.encode("lung cancer immunotherapy");
			float previous = Float.MAX_VALUE;
			for (float weight : sparse.values()) {
				assertTrue(weight <= previous, "weights should be in descending order");
				previous = weight;
			}
		}
	}

	@Test
	@EnabledIfEnvironmentVariable(named = MODEL_DIR_ENV, matches = ".+")
	void testBatchEncode() throws Exception {
		Path modelDir = Path.of(System.getenv(MODEL_DIR_ENV));
		var config = SparseModelConfig.builder("unused").build();
		try (var model = SparseEncoderModel.loadFromDirectory(modelDir, config)) {
			List<String> texts = List.of("lung cancer", "breast cancer", "heart disease");
			List<Map<String, Float>> results = model.batchEncode(texts);
			assertEquals(3, results.size());
			for (Map<String, Float> sparse : results) {
				assertFalse(sparse.isEmpty());
			}
		}
	}

	private static final Set<String> SPECIAL_TOKENS = Set.of(
			"[CLS]", "[SEP]", "[PAD]", "[UNK]", "[MASK]",
			"<s>", "</s>", "<pad>", "<unk>", "<mask>"
	);

	@Test
	@EnabledIfEnvironmentVariable(named = MODEL_DIR_ENV, matches = ".+")
	void testNoSpecialTokens() throws Exception {
		Path modelDir = Path.of(System.getenv(MODEL_DIR_ENV));
		var config = SparseModelConfig.builder("unused").build();
		try (var model = SparseEncoderModel.loadFromDirectory(modelDir, config)) {
			Map<String, Float> sparse = model.encode("lung cancer immunotherapy");
			for (String token : sparse.keySet()) {
				assertFalse(SPECIAL_TOKENS.contains(token),
						"special token should not appear in output: " + token);
			}
		}
	}

	@ParameterizedTest
	@EnumSource(KnownSparseModel.class)
	void testKnownModelEncode(KnownSparseModel knownModel) throws Exception {
		try (var model = SparseEncoderModel.load(knownModel)) {
			Map<String, Float> sparse = model.encode("lung cancer immunotherapy");
			assertNotNull(sparse);
			assertFalse(sparse.isEmpty());
		}
	}
}
