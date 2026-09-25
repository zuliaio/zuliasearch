package io.zulia.ai.sparse;

public record SparseModelConfig(String modelUrl, float weightThreshold, int maxTerms, Boolean includeTokenTypes, int maxTokens) {

	public static final int DEFAULT_MAX_TOKENS = 512;

	public static Builder builder(String modelUrl) {
		return new Builder(modelUrl);
	}

	public static class Builder {

		private final String modelUrl;
		private float weightThreshold = 0f;
		private int maxTerms = 256;
		private Boolean includeTokenTypes;
		private int maxTokens = DEFAULT_MAX_TOKENS;

		private Builder(String modelUrl) {
			this.modelUrl = modelUrl;
		}

		public Builder weightThreshold(float weightThreshold) {
			this.weightThreshold = weightThreshold;
			return this;
		}

		public Builder maxTerms(int maxTerms) {
			this.maxTerms = maxTerms;
			return this;
		}

		public Builder includeTokenTypes(boolean includeTokenTypes) {
			this.includeTokenTypes = includeTokenTypes;
			return this;
		}

		/** Longest input in tokens the model accepts. Longer inputs are truncated to this length. */
		public Builder maxTokens(int maxTokens) {
			this.maxTokens = maxTokens;
			return this;
		}

		public SparseModelConfig build() {
			return new SparseModelConfig(modelUrl, weightThreshold, maxTerms, includeTokenTypes, maxTokens);
		}
	}
}
