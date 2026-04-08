package io.zulia.ai.embedding;

public record EmbeddingModelConfig(String modelUrl, int dimensions, String queryPrefix, String passagePrefix, Integer truncateDimensions) {

	public static EmbeddingModelConfig of(String modelUrl, int dimensions) {
		return new EmbeddingModelConfig(modelUrl, dimensions, "", "", null);
	}

	public static EmbeddingModelConfig withPrefixes(String modelUrl, int dimensions, String queryPrefix, String passagePrefix) {
		return new EmbeddingModelConfig(modelUrl, dimensions, queryPrefix, passagePrefix, null);
	}

	public static EmbeddingModelConfig withTruncation(String modelUrl, int dimensions, int truncateDimensions) {
		return new EmbeddingModelConfig(modelUrl, dimensions, "", "", truncateDimensions);
	}

	public static EmbeddingModelConfig withPrefixesAndTruncation(String modelUrl, int dimensions, String queryPrefix, String passagePrefix,
			int truncateDimensions) {
		return new EmbeddingModelConfig(modelUrl, dimensions, queryPrefix, passagePrefix, truncateDimensions);
	}

	public int outputDimensions() {
		return truncateDimensions != null ? truncateDimensions : dimensions;
	}
}
