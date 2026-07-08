package io.zulia.server.index;

import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.message.ZuliaIndex.IndexAs;
import io.zulia.message.ZuliaIndex.VectorDescription;
import io.zulia.message.ZuliaIndex.VectorIndexingConfig;
import io.zulia.server.config.IndexFieldInfo;
import io.zulia.server.config.ServerIndexConfig;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues.ScalarEncoding;

/**
 * Resolves the effective {@link VectorIndexingConfig} per internal indexed-field name and maps it to Lucene vector
 * formats and similarities, so the codec, indexer, and query path always agree. An explicit encoding is
 * authoritative. {@code ENCODING_UNSPECIFIED} resolves by index-creation version: INT8 at or after
 * {@link ZuliaIndexVersion#VECTOR_DEFAULT_INT8}, raw float32 for older/legacy indexes (no silent re-quantization).
 */
public final class VectorFieldResolver {

	public static final int DEFAULT_HNSW_M = 16;
	public static final int DEFAULT_HNSW_EF_CONSTRUCTION = 100;

	/**
	 * Hard ceiling on the oversampling factor. Bounds the per-shard candidate fetch (ceil(topN * oversample)).
	 */
	public static final float MAX_OVERSAMPLE = 32.0f;

	private VectorFieldResolver() {
	}

	/**
	 * Effective indexing config for an internal indexed field name, never null, with the encoding finalized to a
	 * concrete value so callers never special-case a missing config.
	 */
	public static VectorIndexingConfig resolve(ServerIndexConfig indexConfig, String internalFieldName) {
		VectorIndexingConfig raw = getVectorIndexingConfigForField(indexConfig, internalFieldName);
		VectorIndexingConfig.Encoding effective = effectiveEncoding(indexConfig, raw);
		if (raw == null) {
			return VectorIndexingConfig.newBuilder().setEncoding(effective).build();
		}
		if (raw.getEncoding() == effective) {
			return raw;
		}
		return raw.toBuilder().setEncoding(effective).build();
	}

	private static VectorIndexingConfig getVectorIndexingConfigForField(ServerIndexConfig indexConfig, String internalFieldName) {
		IndexFieldInfo indexFieldInfo = indexConfig.getIndexFieldInfo(internalFieldName);
		if (indexFieldInfo != null) {
			IndexAs indexAs = indexFieldInfo.getIndexAs();
			if (indexAs != null && indexAs.hasVectorIndexingConfig()) {
				return indexAs.getVectorIndexingConfig();
			}
		}
		return null;
	}

	/**
	 * An explicit encoding wins. Null or unspecified resolves to the index-creation-version default.
	 */
	public static VectorIndexingConfig.Encoding effectiveEncoding(ServerIndexConfig indexConfig, VectorIndexingConfig rawConfig) {
		VectorIndexingConfig.Encoding encoding = rawConfig != null ? rawConfig.getEncoding() : VectorIndexingConfig.Encoding.ENCODING_UNSPECIFIED;
		if (encoding == VectorIndexingConfig.Encoding.ENCODING_UNSPECIFIED || encoding == VectorIndexingConfig.Encoding.UNRECOGNIZED) {
			if (indexConfig.getIndexSettings().getCreatedIndexVersion() >= ZuliaIndexVersion.VECTOR_DEFAULT_INT8) {
				return VectorIndexingConfig.Encoding.INT8;
			}
			return VectorIndexingConfig.Encoding.FLOAT32;
		}
		return encoding;
	}

	/**
	 * True when the representation's effective encoding is quantized (anything other than FLOAT32).
	 */
	public static boolean isQuantized(VectorIndexingConfig vectorIndexingConfig) {
		return switch (vectorIndexingConfig.getEncoding()) {
			case INT8, INT7, INT4, BBQ, BBQ_2BIT -> true;
			case FLOAT32, ENCODING_UNSPECIFIED, UNRECOGNIZED -> false;
		};
	}

	/**
	 * Default oversample when the query does not set one. Aggressive BBQ loses too much in the quantized graph to be
	 * usable without full-precision rescoring, so it oversamples by default. 1.0 means no rescore.
	 */
	public static float defaultOversample(VectorIndexingConfig vectorIndexingConfig) {
		return switch (vectorIndexingConfig.getEncoding()) {
			case BBQ -> 3.0f;
			case BBQ_2BIT -> 2.0f;
			case INT4 -> 1.5f;
			case INT8, INT7, FLOAT32, ENCODING_UNSPECIFIED, UNRECOGNIZED -> 1.0f;
		};
	}

	/**
	 * FLOAT32 maps to the raw float32 HNSW format used before quantization existed.
	 */
	public static KnnVectorsFormat buildFormat(VectorIndexingConfig vectorIndexingConfig) {
		if (!isQuantized(vectorIndexingConfig)) {
			return new Lucene99HnswVectorsFormat(hnswM(vectorIndexingConfig), hnswEfConstruction(vectorIndexingConfig));
		}
		ScalarEncoding scalarEncoding = toScalarEncoding(vectorIndexingConfig.getEncoding());
		if (vectorIndexingConfig.getIndexType() == VectorIndexingConfig.IndexType.FLAT) {
			return new Lucene104ScalarQuantizedVectorsFormat(scalarEncoding);
		}
		return new Lucene104HnswScalarQuantizedVectorsFormat(scalarEncoding, hnswM(vectorIndexingConfig), hnswEfConstruction(vectorIndexingConfig));
	}

	/**
	 * DEFAULT derives from the field type (UNIT_VECTOR=dot-product, VECTOR=cosine), matching pre-quantization behavior.
	 */
	public static VectorSimilarityFunction resolveSimilarity(VectorDescription vectorDescription, FieldType fieldType) {
		return switch (vectorDescription.getSimilarity()) {
			case COSINE -> VectorSimilarityFunction.COSINE;
			case DOT_PRODUCT -> VectorSimilarityFunction.DOT_PRODUCT;
			case EUCLIDEAN -> VectorSimilarityFunction.EUCLIDEAN;
			case MAX_INNER_PRODUCT -> VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
			case DEFAULT, UNRECOGNIZED -> fieldType == FieldType.UNIT_VECTOR ? VectorSimilarityFunction.DOT_PRODUCT : VectorSimilarityFunction.COSINE;
		};
	}

	private static int hnswM(VectorIndexingConfig vectorIndexingConfig) {
		return vectorIndexingConfig.getHnswM() > 0 ? vectorIndexingConfig.getHnswM() : DEFAULT_HNSW_M;
	}

	private static int hnswEfConstruction(VectorIndexingConfig vectorIndexingConfig) {
		return vectorIndexingConfig.getHnswEfConstruction() > 0 ? vectorIndexingConfig.getHnswEfConstruction() : DEFAULT_HNSW_EF_CONSTRUCTION;
	}

	private static ScalarEncoding toScalarEncoding(VectorIndexingConfig.Encoding encoding) {
		return switch (encoding) {
			case INT8 -> ScalarEncoding.UNSIGNED_BYTE;
			case INT7 -> ScalarEncoding.SEVEN_BIT;
			case INT4 -> ScalarEncoding.PACKED_NIBBLE;
			case BBQ -> ScalarEncoding.SINGLE_BIT_QUERY_NIBBLE;
			case BBQ_2BIT -> ScalarEncoding.DIBIT_QUERY_NIBBLE;
			case FLOAT32, ENCODING_UNSPECIFIED, UNRECOGNIZED -> throw new IllegalArgumentException("No scalar encoding for vector encoding <" + encoding + ">");
		};
	}
}
