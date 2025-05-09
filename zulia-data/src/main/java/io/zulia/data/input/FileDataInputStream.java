package io.zulia.data.input;

import io.zulia.data.common.DataStreamMeta;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

public class FileDataInputStream implements DataInputStream {

	private final DataStreamMeta dataStreamMeta;
	private final File file;

	public static FileDataInputStream from(String filePath) throws IOException {
		return new FileDataInputStream(filePath);
	}

	private FileDataInputStream(String filePath) throws IOException {
		dataStreamMeta = DataStreamMeta.fromFullPath(filePath);
		file = new File(filePath);
		if (!file.exists()) {
			throw new FileNotFoundException("File <" + filePath + "> does not exist");
		}
		if (file.isDirectory()) {
			throw new IOException("File <" + filePath + "> is a directory");
		}
	}

	@Override
	public InputStream openInputStream() throws IOException {

		InputStream in = new FileInputStream(file);
		if (DataStreamMeta.isGzipExtension(file.getName())) {
			in = new GZIPInputStream(in);
		}
		return new BufferedInputStream(in);
	}

	@Override
	public DataStreamMeta getMeta() {
		return dataStreamMeta;
	}
}
