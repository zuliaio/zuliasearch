package io.zulia.ai.embedding;

public enum KnownEmbeddingModel {

	// E5 English models - asymmetric (query/passage prefixes), mean pooling
	E5_SMALL_V2(EmbeddingModelConfig.builder("djl://ai.djl.huggingface.onnxruntime/intfloat/e5-small-v2", 384)
			.prefixes("query: ", "passage: ").build()),

	E5_BASE_V2(EmbeddingModelConfig.builder("djl://ai.djl.huggingface.onnxruntime/intfloat/e5-base-v2", 768)
			.prefixes("query: ", "passage: ").build()),

	E5_LARGE_V2(EmbeddingModelConfig.builder("djl://ai.djl.huggingface.onnxruntime/intfloat/e5-large-v2", 1024)
			.prefixes("query: ", "passage: ").build()),

	// E5 Multilingual models - asymmetric, 100+ languages, mean pooling
	MULTILINGUAL_E5_SMALL(EmbeddingModelConfig.builder("djl://ai.djl.huggingface.onnxruntime/intfloat/multilingual-e5-small", 384)
			.prefixes("query: ", "passage: ").build()),

	MULTILINGUAL_E5_BASE(EmbeddingModelConfig.builder("djl://ai.djl.huggingface.onnxruntime/intfloat/multilingual-e5-base", 768)
			.prefixes("query: ", "passage: ").build()),

	MULTILINGUAL_E5_LARGE(EmbeddingModelConfig.builder("djl://ai.djl.huggingface.onnxruntime/intfloat/multilingual-e5-large", 1024)
			.prefixes("query: ", "passage: ").build()),

	// Sentence Transformers - symmetric, general purpose, mean pooling
	ALL_MINILM_L6_V2(EmbeddingModelConfig.builder("djl://ai.djl.huggingface.onnxruntime/sentence-transformers/all-MiniLM-L6-v2", 384).build()),

	// Biomedical domain - symmetric, mean pooling
	PUBMEDBERT(EmbeddingModelConfig.builder("djl://ai.djl.huggingface.onnxruntime/NeuML/pubmedbert-base-embeddings", 768).build()),

	// Jina v2 - symmetric, 8192 token context, mean pooling
	JINA_SMALL_EN_V2(EmbeddingModelConfig.builder("djl://ai.djl.huggingface.onnxruntime/jinaai/jina-embeddings-v2-small-en", 512).build()),

	JINA_BASE_EN_V2(EmbeddingModelConfig.builder("https://huggingface.co/jinaai/jina-embeddings-v2-base-en", 768)
			.includeTokenTypes(true).build()),

	// BGE English models - instruction-tuned, CLS pooling
	BGE_SMALL_EN_V1_5(EmbeddingModelConfig.builder("https://huggingface.co/BAAI/bge-small-en-v1.5", 384)
			.prefixes("Represent this sentence for searching relevant passages: ", "")
			.includeTokenTypes(true).poolingMode("cls").build()),

	BGE_BASE_EN_V1_5(EmbeddingModelConfig.builder("https://huggingface.co/BAAI/bge-base-en-v1.5", 768)
			.prefixes("Represent this sentence for searching relevant passages: ", "")
			.includeTokenTypes(true).poolingMode("cls").build()),

	// Nomic Embed - 8192 token context, Matryoshka support, Apache 2.0 license, mean pooling
	NOMIC_EMBED_TEXT_V1_5(EmbeddingModelConfig.builder("https://huggingface.co/nomic-ai/nomic-embed-text-v1.5", 768)
			.prefixes("search_query: ", "search_document: ")
			.includeTokenTypes(true).build()),

	NOMIC_EMBED_TEXT_V1_5_512(EmbeddingModelConfig.builder("https://huggingface.co/nomic-ai/nomic-embed-text-v1.5", 768)
			.prefixes("search_query: ", "search_document: ")
			.includeTokenTypes(true).truncateDimensions(512).build()),

	NOMIC_EMBED_TEXT_V1_5_256(EmbeddingModelConfig.builder("https://huggingface.co/nomic-ai/nomic-embed-text-v1.5", 768)
			.prefixes("search_query: ", "search_document: ")
			.includeTokenTypes(true).truncateDimensions(256).build()),

	// Snowflake Arctic Embed - retrieval-optimized, CLS pooling
	SNOWFLAKE_ARCTIC_EMBED_M_V2(EmbeddingModelConfig.builder("https://huggingface.co/Snowflake/snowflake-arctic-embed-m-v2.0", 768)
			.prefixes("Represent this sentence for searching relevant passages: ", "")
			.poolingMode("cls").build()),

	// Bioclinical ModernBERT - biomedical/clinical domain, mean pooling, 8192 token context
	BIOCLINICAL_MODERNBERT(EmbeddingModelConfig.builder("https://huggingface.co/zuliaio/bioclinical-modernbert-base-embeddings-onnx", 768)
			.build());

	private final EmbeddingModelConfig config;

	KnownEmbeddingModel(EmbeddingModelConfig config) {
		this.config = config;
	}

	public EmbeddingModelConfig getConfig() {
		return config;
	}

}
