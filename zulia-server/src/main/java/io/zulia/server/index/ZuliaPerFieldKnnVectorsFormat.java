package io.zulia.server.index;

import io.zulia.server.config.ServerIndexConfig;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;

import java.util.Objects;

/**
 * Write path only. Selects the Lucene KNN vectors format per field from each representation's resolved
 * {@link io.zulia.message.ZuliaIndex.VectorIndexingConfig}, so the codec, indexer, and query path always agree.
 */
final class ZuliaPerFieldKnnVectorsFormat extends PerFieldKnnVectorsFormat {

	private final ServerIndexConfig indexConfig;

	ZuliaPerFieldKnnVectorsFormat(ServerIndexConfig indexConfig) {
		this.indexConfig = Objects.requireNonNull(indexConfig, "indexConfig is required for the write codec");
	}

	@Override
	public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
		return VectorFieldResolver.buildFormat(VectorFieldResolver.resolve(indexConfig, field));
	}
}
