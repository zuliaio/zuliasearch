package io.zulia.server.index;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;

/**
 * Zulia read codec bound permanently to {@link Lucene104Codec} formats (there is no 10.5 codec) under the persisted
 * name {@code "Zulia104"}. The no-arg constructor is the SPI entry point used when opening existing segments. Stored
 * per-field format names are resolved by SPI on read, so no index config is needed and this instance can read any
 * Zulia104 segment. New segments are written with {@link ZuliaLucene104WriteCodec} (see {@link ZuliaCodec}).
 */
public sealed class ZuliaLucene104Codec extends ZuliaCodec permits ZuliaLucene104WriteCodec {

	public static final String CODEC_NAME = "Zulia104";

	public ZuliaLucene104Codec() {
		this(new ReadOnlyPerFieldKnnVectorsFormat(CODEC_NAME));
	}

	protected ZuliaLucene104Codec(KnnVectorsFormat knnVectorsFormat) {
		super(CODEC_NAME, new Lucene104Codec(), knnVectorsFormat);
	}
}
