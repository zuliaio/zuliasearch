package io.zulia.server.index;

import io.zulia.message.ZuliaIndex.VectorIndexingConfig;
import io.zulia.server.config.ServerIndexConfig;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;

/**
 * Selects the Lucene KNN vectors format per field from each representation's resolved {@link VectorIndexingConfig}.
 * The read path resolves stored format names via SPI and never calls {@link #getKnnVectorsFormatForField(String)},
 * so the config-less SPI instance can read any segment.
 */
public class ZuliaPerFieldKnnVectorsFormat extends PerFieldKnnVectorsFormat {

	private final ServerIndexConfig indexConfig;

	/** SPI read path only. */
	public ZuliaPerFieldKnnVectorsFormat() {
		this(null);
	}

	public ZuliaPerFieldKnnVectorsFormat(ServerIndexConfig indexConfig) {
		this.indexConfig = indexConfig;
	}

	@Override
	public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
		VectorIndexingConfig vectorIndexingConfig = indexConfig != null ? VectorFieldResolver.resolve(indexConfig, field) : null;
		return VectorFieldResolver.buildFormat(vectorIndexingConfig);
	}
}
