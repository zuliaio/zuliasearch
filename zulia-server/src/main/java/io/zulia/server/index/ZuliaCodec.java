package io.zulia.server.index;

import io.zulia.server.config.ServerIndexConfig;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;

/**
 * Base for Zulia's per-Lucene-version codecs. Each subclass pins one persisted codec name to one Lucene delegate
 * ({@link ZuliaLucene104Codec} = "Zulia104" = Lucene104 formats) and overrides only per-field KNN vector formats.
 *
 * <p>Segments persist their codec name and Lucene resolves it back via SPI, so the name to delegate binding is
 * permanent. Never repoint or rename a released subclass. For a newer Lucene default codec, add a new subclass,
 * register it in {@code META-INF/services}, point {@link #currentWriteCodec} at it, and keep the old ones registered
 * so existing segments stay readable. Every node needs the write codec on its classpath before any node writes
 * vector segments (replication and restarts open segments by persisted codec name).
 */
public abstract class ZuliaCodec extends FilterCodec {

	private final KnnVectorsFormat knnVectorsFormat;

	protected ZuliaCodec(String codecName, Codec delegate, ServerIndexConfig indexConfig) {
		super(codecName, delegate);
		this.knnVectorsFormat = new ZuliaPerFieldKnnVectorsFormat(indexConfig);
	}

	/** The codec new segments are written with. The single place to update on a Lucene default-codec upgrade. */
	public static ZuliaCodec currentWriteCodec(ServerIndexConfig indexConfig) {
		return new ZuliaLucene104Codec(indexConfig);
	}

	@Override
	public KnnVectorsFormat knnVectorsFormat() {
		return knnVectorsFormat;
	}
}
