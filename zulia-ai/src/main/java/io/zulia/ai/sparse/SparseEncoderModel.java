package io.zulia.ai.sparse;

import ai.djl.MalformedModelException;
import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import ai.djl.inference.Predictor;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ModelNotFoundException;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.translate.TranslateException;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.zulia.ai.embedding.HuggingFaceModelDownloader;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SparseEncoderModel implements AutoCloseable {

	private final ZooModel<String, Map<String, Float>> model;
	private final SparseModelConfig config;

	private SparseEncoderModel(ZooModel<String, Map<String, Float>> model, SparseModelConfig config) {
		this.model = model;
		this.config = config;
	}

	public static SparseEncoderModel load(KnownSparseModel knownModel) throws ModelNotFoundException, MalformedModelException, IOException {
		return load(knownModel.getConfig());
	}

	public static SparseEncoderModel load(SparseModelConfig config) throws ModelNotFoundException, MalformedModelException, IOException {
		Path modelDir = HuggingFaceModelDownloader.downloadModel(config.modelUrl());
		return loadFromDirectory(modelDir, config);
	}

	@SuppressWarnings("unchecked")
	public static SparseEncoderModel loadFromDirectory(Path modelDir, SparseModelConfig config)
			throws ModelNotFoundException, MalformedModelException, IOException {
		HuggingFaceTokenizer tokenizer = HuggingFaceTokenizer.newInstance(modelDir);
		String[] vocabulary = loadVocabulary(modelDir);

		boolean includeTokenTypes = config.includeTokenTypes() != null ? config.includeTokenTypes()
				: detectIncludeTokenTypes(modelDir);

		var translator = new SparseTranslator(tokenizer, vocabulary, config.weightThreshold(), config.maxTerms(),
				includeTokenTypes);

		Criteria<String, Map<String, Float>> criteria = Criteria.builder()
				.setTypes(String.class, (Class<Map<String, Float>>) (Class<?>) Map.class)
				.optModelPath(modelDir)
				.optEngine("OnnxRuntime")
				.optTranslator(translator)
				.build();

		ZooModel<String, Map<String, Float>> model = criteria.loadModel();
		return new SparseEncoderModel(model, config);
	}

	public Map<String, Float> encode(String text) throws TranslateException {
		try (Predictor<String, Map<String, Float>> predictor = model.newPredictor()) {
			return predictor.predict(text);
		}
	}

	public List<Map<String, Float>> batchEncode(List<String> texts) throws TranslateException {
		var results = new ArrayList<Map<String, Float>>(texts.size());
		try (Predictor<String, Map<String, Float>> predictor = model.newPredictor()) {
			for (String text : texts) {
				results.add(predictor.predict(text));
			}
		}
		return results;
	}

	public SparseModelConfig getConfig() {
		return config;
	}

	@Override
	public void close() {
		model.close();
	}

	private static boolean detectIncludeTokenTypes(Path modelDir) throws IOException {
		Path configPath = modelDir.resolve("config.json");
		if (!Files.exists(configPath)) {
			return true;
		}
		try (Reader reader = Files.newBufferedReader(configPath)) {
			JsonObject config = JsonParser.parseReader(reader).getAsJsonObject();
			if (config.has("model_type")) {
				String modelType = config.get("model_type").getAsString();
				return !Set.of("distilbert", "roberta", "xlnet").contains(modelType);
			}
		}
		return true;
	}

	private static String[] loadVocabulary(Path modelDir) throws IOException {
		Path tokenizerPath = modelDir.resolve("tokenizer.json");
		try (Reader reader = Files.newBufferedReader(tokenizerPath)) {
			JsonObject root = JsonParser.parseReader(reader).getAsJsonObject();
			JsonObject vocab = root.getAsJsonObject("model").getAsJsonObject("vocab");

			String[] vocabulary = new String[vocab.size()];
			for (Map.Entry<String, JsonElement> entry : vocab.entrySet()) {
				int id = entry.getValue().getAsInt();
				if (id >= 0 && id < vocabulary.length) {
					vocabulary[id] = entry.getKey();
				}
			}
			return vocabulary;
		}
	}
}
