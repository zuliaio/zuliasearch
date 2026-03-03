package io.zulia.ai.embedding;

public record EmbeddingModelConfig(String modelUrl, int dimensions, String queryPrefix, String passagePrefix) {

	public static EmbeddingModelConfig of(String modelUrl, int dimensions) {
		return new EmbeddingModelConfig(modelUrl, dimensions, "", "");
	}

	public static EmbeddingModelConfig withPrefixes(String modelUrl, int dimensions, String queryPrefix, String passagePrefix) {
		return new EmbeddingModelConfig(modelUrl, dimensions, queryPrefix, passagePrefix);
	}

}
