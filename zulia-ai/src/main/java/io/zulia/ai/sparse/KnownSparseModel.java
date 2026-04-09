package io.zulia.ai.sparse;

public enum KnownSparseModel {

	// OpenSearch Neural Sparse v1 - symmetric, BERT-base (110M params), 0.524 BEIR NDCG@10
	OPENSEARCH_NEURAL_SPARSE_V1(SparseModelConfig.builder("https://huggingface.co/zuliaio/opensearch-sparse-v1-onnx").build()),

	// OpenSearch Neural Sparse v2 Distill - symmetric, DistilBERT (67M params), 0.528 BEIR NDCG@10
	OPENSEARCH_NEURAL_SPARSE_V2_DISTILL(SparseModelConfig.builder("https://huggingface.co/zuliaio/opensearch-sparse-v2-distill-onnx")
			.includeTokenTypes(false).build()),

	// IBM Granite Sparse - symmetric, RoBERTa (30M params), 0.508 BEIR NDCG@10, BPE tokenizer (50K vocab)
	GRANITE_30M_SPARSE(SparseModelConfig.builder("https://huggingface.co/zuliaio/granite-30m-sparse-onnx")
			.includeTokenTypes(false).build());

	private final SparseModelConfig config;

	KnownSparseModel(SparseModelConfig config) {
		this.config = config;
	}

	public SparseModelConfig getConfig() {
		return config;
	}
}
