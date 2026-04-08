package io.zulia.ai.embedding;

import ai.djl.MalformedModelException;
import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import ai.djl.huggingface.translator.TextEmbeddingTranslator;
import ai.djl.inference.Predictor;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ModelNotFoundException;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.translate.TranslateException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TextEmbeddingModel implements AutoCloseable {

	private final ZooModel<String, float[]> model;
	private final EmbeddingModelConfig config;

	private TextEmbeddingModel(ZooModel<String, float[]> model, EmbeddingModelConfig config) {
		this.model = model;
		this.config = config;
	}

	public static TextEmbeddingModel load(KnownEmbeddingModel knownModel) throws ModelNotFoundException, MalformedModelException, IOException {
		return load(knownModel.getConfig());
	}

	public static TextEmbeddingModel load(EmbeddingModelConfig config) throws ModelNotFoundException, MalformedModelException, IOException {
		if (config.modelUrl().startsWith("djl://")) {
			return loadFromZoo(config);
		}
		return loadFromHuggingFace(config);
	}

	private static TextEmbeddingModel loadFromZoo(EmbeddingModelConfig config) throws ModelNotFoundException, MalformedModelException, IOException {
		Criteria<String, float[]> criteria = Criteria.builder()
				.setTypes(String.class, float[].class)
				.optModelUrls(config.modelUrl())
				.optEngine("OnnxRuntime")
				.build();

		ZooModel<String, float[]> model = criteria.loadModel();
		return new TextEmbeddingModel(model, config);
	}

	private static TextEmbeddingModel loadFromHuggingFace(EmbeddingModelConfig config) throws ModelNotFoundException, MalformedModelException, IOException {
		Path modelDir = HuggingFaceModelDownloader.downloadModel(config.modelUrl());

		HuggingFaceTokenizer tokenizer = HuggingFaceTokenizer.newInstance(modelDir);

		TextEmbeddingTranslator translator = TextEmbeddingTranslator.builder(tokenizer)
				.optPoolingMode(config.poolingModeOrDefault())
				.optNormalize(true)
				.optIncludeTokenTypes(config.includeTokenTypes())
				.build();

		Criteria<String, float[]> criteria = Criteria.builder()
				.setTypes(String.class, float[].class)
				.optModelPath(modelDir)
				.optEngine("OnnxRuntime")
				.optTranslator(translator)
				.build();

		ZooModel<String, float[]> model = criteria.loadModel();
		return new TextEmbeddingModel(model, config);
	}

	public float[] embed(String text) throws TranslateException {
		try (Predictor<String, float[]> predictor = model.newPredictor()) {
			return truncate(predictor.predict(text));
		}
	}

	public float[] embedQuery(String text) throws TranslateException {
		return embed(config.queryPrefix() + text);
	}

	public float[] embedPassage(String text) throws TranslateException {
		return embed(config.passagePrefix() + text);
	}

	public List<float[]> batchEmbed(List<String> texts) throws TranslateException {
		try (Predictor<String, float[]> predictor = model.newPredictor()) {
			List<float[]> results = predictor.batchPredict(texts);
			if (config.truncateDimensions() != null) {
				return results.stream().map(this::truncate).toList();
			}
			return results;
		}
	}

	public List<float[]> batchEmbedQuery(List<String> texts) throws TranslateException {
		List<String> prefixed = new ArrayList<>(texts.size());
		for (String text : texts) {
			prefixed.add(config.queryPrefix() + text);
		}
		return batchEmbed(prefixed);
	}

	public List<float[]> batchEmbedPassage(List<String> texts) throws TranslateException {
		List<String> prefixed = new ArrayList<>(texts.size());
		for (String text : texts) {
			prefixed.add(config.passagePrefix() + text);
		}
		return batchEmbed(prefixed);
	}

	public Predictor<String, float[]> newPredictor() {
		return model.newPredictor();
	}

	public int getDimensions() {
		return config.outputDimensions();
	}

	public EmbeddingModelConfig getConfig() {
		return config;
	}

	private float[] truncate(float[] vector) {
		if (config.truncateDimensions() != null && vector.length > config.truncateDimensions()) {
			return Arrays.copyOf(vector, config.truncateDimensions());
		}
		return vector;
	}

	@Override
	public void close() {
		model.close();
	}

}
