package io.zulia.fields;

import static io.zulia.message.ZuliaIndex.VectorIndexingConfig;

/**
 * Builds a {@link VectorIndexingConfig} for one indexed representation of a vector field, e.g.
 * {@code FieldConfigBuilder.createVector("v").indexAs("vBBQ", VectorIndexingConfigBuilder.create().quantization(Encoding.BBQ).hnsw(24, 200))}.
 */
public class VectorIndexingConfigBuilder {

	private VectorIndexingConfig.Encoding encoding;
	private VectorIndexingConfig.IndexType indexType;
	private Integer hnswM;
	private Integer hnswEfConstruction;

	public static VectorIndexingConfigBuilder create() {
		return new VectorIndexingConfigBuilder();
	}

	private VectorIndexingConfigBuilder() {
	}

	/** Vector encoding for this representation. Left unset, the server picks the index-creation-version default. */
	public VectorIndexingConfigBuilder quantization(VectorIndexingConfig.Encoding encoding) {
		this.encoding = encoding;
		return this;
	}

	/** HNSW graph tuning (m = max connections, efConstruction = build beam width). */
	public VectorIndexingConfigBuilder hnsw(int m, int efConstruction) {
		this.hnswM = m;
		this.hnswEfConstruction = efConstruction;
		return this;
	}

	/** Exact brute-force (flat) index instead of the default HNSW graph. */
	public VectorIndexingConfigBuilder flat() {
		this.indexType = VectorIndexingConfig.IndexType.FLAT;
		return this;
	}

	/** Use HNSW graph (the default, but can be used to toggle back from flat()) **/
	public VectorIndexingConfigBuilder hnsw() {
		this.indexType = VectorIndexingConfig.IndexType.HNSW;
		return this;
	}

	public VectorIndexingConfig build() {
		VectorIndexingConfig.Builder builder = VectorIndexingConfig.newBuilder();
		if (encoding != null) {
			builder.setEncoding(encoding);
		}
		if (indexType != null) {
			builder.setIndexType(indexType);
		}
		if (hnswM != null) {
			builder.setHnswM(hnswM);
		}
		if (hnswEfConstruction != null) {
			builder.setHnswEfConstruction(hnswEfConstruction);
		}
		return builder.build();
	}
}
