package io.zulia.ai.embedding;

public record EmbeddingModelConfig(String modelUrl, int dimensions, String queryPrefix, String passagePrefix, Integer truncateDimensions,
		boolean includeTokenTypes, String poolingMode, int maxTokens) {

	/** Sequence length most BERT style encoders were trained with, and the DJL tokenizer default. */
	public static final int DEFAULT_MAX_TOKENS = 512;

	public static Builder builder(String modelUrl, int dimensions) {
		return new Builder(modelUrl, dimensions);
	}

	public int outputDimensions() {
		return truncateDimensions != null ? truncateDimensions : dimensions;
	}

	public String poolingModeOrDefault() {
		return poolingMode != null ? poolingMode : "mean";
	}

	public static class Builder {

		private final String modelUrl;
		private final int dimensions;
		private String queryPrefix = "";
		private String passagePrefix = "";
		private Integer truncateDimensions;
		private boolean includeTokenTypes;
		private String poolingMode;
		private int maxTokens = DEFAULT_MAX_TOKENS;

		private Builder(String modelUrl, int dimensions) {
			this.modelUrl = modelUrl;
			this.dimensions = dimensions;
		}

		public Builder prefixes(String queryPrefix, String passagePrefix) {
			this.queryPrefix = queryPrefix;
			this.passagePrefix = passagePrefix;
			return this;
		}

		public Builder truncateDimensions(int truncateDimensions) {
			this.truncateDimensions = truncateDimensions;
			return this;
		}

		public Builder includeTokenTypes(boolean includeTokenTypes) {
			this.includeTokenTypes = includeTokenTypes;
			return this;
		}

		public Builder poolingMode(String poolingMode) {
			this.poolingMode = poolingMode;
			return this;
		}

		/** Longest input in tokens the model accepts. Longer inputs are truncated to this length. */
		public Builder maxTokens(int maxTokens) {
			this.maxTokens = maxTokens;
			return this;
		}

		public EmbeddingModelConfig build() {
			return new EmbeddingModelConfig(modelUrl, dimensions, queryPrefix, passagePrefix, truncateDimensions, includeTokenTypes, poolingMode, maxTokens);
		}
	}
}
