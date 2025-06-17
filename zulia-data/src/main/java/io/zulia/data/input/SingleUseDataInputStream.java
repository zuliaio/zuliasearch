package io.zulia.data.input;

import io.zulia.data.common.DataStreamMeta;

import java.io.IOException;
import java.io.InputStream;

public class SingleUseDataInputStream implements DataInputStream {

	private final DataStreamMeta meta;
	private InputStream inputStream;

	public static SingleUseDataInputStream from(InputStream inputStream, DataStreamMeta meta) throws IOException {
		return new SingleUseDataInputStream(inputStream, meta);
	}

	public static SingleUseDataInputStream from(InputStream inputStream, String filename) throws IOException {
		return new SingleUseDataInputStream(inputStream, DataStreamMeta.fromFileName(filename));
	}

	public static SingleUseDataInputStream from(InputStream inputStream, String contentType, String filename) throws IOException {
		return new SingleUseDataInputStream(inputStream, new DataStreamMeta(contentType, filename));
	}

	private SingleUseDataInputStream(InputStream inputStream, DataStreamMeta meta) {
		this.inputStream = inputStream;
		this.meta = meta;
	}

	@Override
	public InputStream openInputStream() throws IOException {
		if (inputStream == null) {
			throw new IOException("SingleUseDataInputStream can only be used once");
		}
		InputStream ret = inputStream;
		inputStream = null;
		return ret;
	}

	@Override
	public DataStreamMeta getMeta() {
		return meta;
	}
}
