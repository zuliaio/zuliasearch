package io.zulia.server.index;

import io.zulia.server.config.ServerIndexConfig;

/**
 * Write codec for {@code "Zulia104"} segments. Extends the read codec so new segments persist that name and read
 * back through SPI without this class or the index config. Only reachable via {@link ZuliaCodec#currentWriteCodec}.
 */
public final class ZuliaLucene104WriteCodec extends ZuliaLucene104Codec {

	public ZuliaLucene104WriteCodec(ServerIndexConfig indexConfig) {
		super(new ZuliaPerFieldKnnVectorsFormat(indexConfig));
	}
}
