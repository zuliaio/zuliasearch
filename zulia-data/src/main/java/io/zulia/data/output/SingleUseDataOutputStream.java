package io.zulia.data.output;

import io.zulia.data.common.DataStreamMeta;

import java.io.IOException;
import java.io.OutputStream;

public class SingleUseDataOutputStream implements DataOutputStream {

	private final DataStreamMeta meta;
	private OutputStream outputStream;

	public static SingleUseDataOutputStream from(OutputStream outputStream, DataStreamMeta meta) throws IOException {
		return new SingleUseDataOutputStream(outputStream, meta);
	}

	public static SingleUseDataOutputStream from(OutputStream outputStream, String filename) throws IOException {
		return new SingleUseDataOutputStream(outputStream, DataStreamMeta.fromFileName(filename));
	}

	public static SingleUseDataOutputStream from(OutputStream outputStream, String contentType, String filename) throws IOException {
		return new SingleUseDataOutputStream(outputStream, new DataStreamMeta(contentType, filename));
	}

	private SingleUseDataOutputStream(OutputStream outputStream, DataStreamMeta meta) {
		this.outputStream = outputStream;
		this.meta = meta;
	}

	@Override
	public synchronized OutputStream openOutputStream() throws IOException {
		if (outputStream == null) {
			throw new IOException("SingleUseDataOutputStream can only be used once");
		}
		OutputStream ret = outputStream;
		outputStream = null;
		return ret;
	}

	@Override
	public DataStreamMeta getMeta() {
		return meta;
	}
}
