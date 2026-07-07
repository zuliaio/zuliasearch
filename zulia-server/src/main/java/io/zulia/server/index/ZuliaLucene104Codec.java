package io.zulia.server.index;

import io.zulia.server.config.ServerIndexConfig;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;

/**
 * Zulia codec bound permanently to {@link Lucene104Codec} formats (there is no 10.5 codec) under the persisted name
 * {@code "Zulia104"}. For a newer Lucene default codec, add a sibling class rather than changing this one (see {@link ZuliaCodec}).
 */
public final class ZuliaLucene104Codec extends ZuliaCodec {

	public static final String CODEC_NAME = "Zulia104";

	/** SPI read-path constructor. Per-field formats are resolved by their stored name, so no config is needed. */
	public ZuliaLucene104Codec() {
		this(null);
	}

	public ZuliaLucene104Codec(ServerIndexConfig indexConfig) {
		super(CODEC_NAME, new Lucene104Codec(), indexConfig);
	}
}
