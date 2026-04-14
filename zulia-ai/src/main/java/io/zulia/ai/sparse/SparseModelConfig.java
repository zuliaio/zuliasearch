package io.zulia.ai.sparse;

public record SparseModelConfig(String modelUrl, float weightThreshold, int maxTerms, Boolean includeTokenTypes) {

	public static Builder builder(String modelUrl) {
		return new Builder(modelUrl);
	}

	public static class Builder {

		private final String modelUrl;
		private float weightThreshold = 0f;
		private int maxTerms = 256;
		private Boolean includeTokenTypes;

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

		public SparseModelConfig build() {
			return new SparseModelConfig(modelUrl, weightThreshold, maxTerms, includeTokenTypes);
		}
	}
}
