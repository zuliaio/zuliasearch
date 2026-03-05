package io.zulia.ai.embedding;

public enum KnownEmbeddingModel {

	E5_SMALL_V2(EmbeddingModelConfig.withPrefixes(
			"djl://ai.djl.huggingface.onnxruntime/intfloat/e5-small-v2", 384,
			"query: ", "passage: "
	)),

	ALL_MINILM_L6_V2(EmbeddingModelConfig.of(
			"djl://ai.djl.huggingface.onnxruntime/sentence-transformers/all-MiniLM-L6-v2", 384
	)),

	PUBMEDBERT(EmbeddingModelConfig.of(
			"djl://ai.djl.huggingface.onnxruntime/NeuML/pubmedbert-base-embeddings", 768
	));

	private final EmbeddingModelConfig config;

	KnownEmbeddingModel(EmbeddingModelConfig config) {
		this.config = config;
	}

	public EmbeddingModelConfig getConfig() {
		return config;
	}

}
