package io.zulia.server.index;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;

/**
 * Read path only. {@link PerFieldKnnVectorsFormat}'s reader resolves each field's stored format name via SPI and
 * never calls {@link #getKnnVectorsFormatForField(String)}, so an SPI-loaded codec can read any segment. Writing
 * through it fails fast instead of silently using default formats.
 */
final class ReadOnlyPerFieldKnnVectorsFormat extends PerFieldKnnVectorsFormat {

	private final String codecName;

	ReadOnlyPerFieldKnnVectorsFormat(String codecName) {
		this.codecName = codecName;
	}

	@Override
	public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
		throw new IllegalStateException("Codec <" + codecName
				+ "> was loaded via SPI and is read-only. New segments must be written with ZuliaCodec.currentWriteCodec, requested field <" + field + ">");
	}
}
