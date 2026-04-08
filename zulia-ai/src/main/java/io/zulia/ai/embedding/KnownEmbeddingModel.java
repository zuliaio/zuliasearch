package io.zulia.ai.embedding;

public enum KnownEmbeddingModel {

	// E5 English models - asymmetric (query/passage prefixes)
	E5_SMALL_V2(EmbeddingModelConfig.withPrefixes(
			"djl://ai.djl.huggingface.onnxruntime/intfloat/e5-small-v2", 384,
			"query: ", "passage: "
	)),

	E5_BASE_V2(EmbeddingModelConfig.withPrefixes(
			"djl://ai.djl.huggingface.onnxruntime/intfloat/e5-base-v2", 768,
			"query: ", "passage: "
	)),

	E5_LARGE_V2(EmbeddingModelConfig.withPrefixes(
			"djl://ai.djl.huggingface.onnxruntime/intfloat/e5-large-v2", 1024,
			"query: ", "passage: "
	)),

	// E5 Multilingual models - asymmetric, 100+ languages
	MULTILINGUAL_E5_SMALL(EmbeddingModelConfig.withPrefixes(
			"djl://ai.djl.huggingface.onnxruntime/intfloat/multilingual-e5-small", 384,
			"query: ", "passage: "
	)),

	MULTILINGUAL_E5_BASE(EmbeddingModelConfig.withPrefixes(
			"djl://ai.djl.huggingface.onnxruntime/intfloat/multilingual-e5-base", 768,
			"query: ", "passage: "
	)),

	MULTILINGUAL_E5_LARGE(EmbeddingModelConfig.withPrefixes(
			"djl://ai.djl.huggingface.onnxruntime/intfloat/multilingual-e5-large", 1024,
			"query: ", "passage: "
	)),

	// Sentence Transformers - symmetric, general purpose
	ALL_MINILM_L6_V2(EmbeddingModelConfig.of(
			"djl://ai.djl.huggingface.onnxruntime/sentence-transformers/all-MiniLM-L6-v2", 384
	)),

	// Biomedical domain - symmetric
	PUBMEDBERT(EmbeddingModelConfig.of(
			"djl://ai.djl.huggingface.onnxruntime/NeuML/pubmedbert-base-embeddings", 768
	)),

	// Jina v2 small - symmetric, 8192 token context, 33M params
	JINA_SMALL_EN_V2(EmbeddingModelConfig.of(
			"djl://ai.djl.huggingface.onnxruntime/jinaai/jina-embeddings-v2-small-en", 512
	));

	private final EmbeddingModelConfig config;

	KnownEmbeddingModel(EmbeddingModelConfig config) {
		this.config = config;
	}

	public EmbeddingModelConfig getConfig() {
		return config;
	}

}
