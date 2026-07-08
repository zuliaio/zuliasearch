package io.zulia.server.index;

import io.zulia.server.config.ServerIndexConfig;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;

/**
 * Base for Zulia's per-Lucene-version codecs. Each version is a read/write pair: the read class pins one persisted
 * codec name to one Lucene delegate ({@link ZuliaLucene104Codec} = "Zulia104" = Lucene104 formats) and is what SPI
 * resolves when opening segments, and its write subclass ({@link ZuliaLucene104WriteCodec}) adds the index config
 * for choosing per-field KNN vector formats on new segments.
 *
 * <p>Segments persist their codec name and Lucene resolves it back via SPI, so the name to delegate binding is
 * permanent. Never repoint or rename a released version class. For a newer Lucene default codec, add a new read/write
 * pair, register the read class in {@code META-INF/services}, and point {@link #currentWriteCodec} at the new write
 * class. Keep the old read classes registered so existing segments stay readable. Every node needs the read codec on
 * its classpath before any node writes vector segments (replication and restarts open segments by persisted codec name).
 */
public abstract class ZuliaCodec extends FilterCodec {

	private final KnnVectorsFormat knnVectorsFormat;

	protected ZuliaCodec(String codecName, Codec delegate, KnnVectorsFormat knnVectorsFormat) {
		super(codecName, delegate);
		this.knnVectorsFormat = knnVectorsFormat;
	}

	/** The codec new segments are written with. The single place to update on a Lucene default-codec upgrade. */
	public static ZuliaCodec currentWriteCodec(ServerIndexConfig indexConfig) {
		return new ZuliaLucene104WriteCodec(indexConfig);
	}

	@Override
	public KnnVectorsFormat knnVectorsFormat() {
		return knnVectorsFormat;
	}
}
