package io.zulia.data.output;

import io.zulia.data.DataStreamMeta;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

public class FileDataOutputStream implements DataOutputStream {

	private final DataStreamMeta dataStreamMeta;
	private final File file;

	public static FileDataOutputStream from(String filePath, boolean overwrite) throws IOException {
		return new FileDataOutputStream(filePath, overwrite);
	}

	private FileDataOutputStream(String filePath, boolean overwrite) throws IOException {
		dataStreamMeta = DataStreamMeta.fromFullPath(filePath);
		file = new File(filePath);
		if (!overwrite && file.exists()) {
			throw new FileNotFoundException("File <" + filePath + "> exists and overwrite is false");
		}
		if (file.isDirectory()) {
			throw new IOException("Cannot write file.  File <" + filePath + "> is a directory");
		}
	}

	@Override
	public OutputStream openOutputStream() throws IOException {

		OutputStream out = new FileOutputStream(file);
		if (DataStreamMeta.isGzipExtension(file.getName())) {
			out = new GZIPOutputStream(out);
		}
		return new BufferedOutputStream(out);
	}

	@Override
	public DataStreamMeta getMeta() {
		return dataStreamMeta;
	}
}
