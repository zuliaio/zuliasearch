package io.zulia.data.input;

import io.zulia.data.common.DataStreamMeta;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

public interface DataInputStream {

	/**
	 * Provides the underlying stream. Implementations return the raw stream, which may or may not be buffered.
	 */
	InputStream openRawInputStream() throws IOException;

	/**
	 * Returns a stream safe for direct reads. The returned stream is buffered; when the raw stream is already a
	 * {@link BufferedInputStream} it is returned unchanged to avoid double buffering.
	 */
	default InputStream openInputStream() throws IOException {
		InputStream in = openRawInputStream();
		return in instanceof BufferedInputStream ? in : new BufferedInputStream(in);
	}

	DataStreamMeta getMeta();

}
