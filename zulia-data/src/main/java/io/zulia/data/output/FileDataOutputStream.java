package io.zulia.data.output;

import io.zulia.data.common.DataStreamMeta;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;

public class FileDataOutputStream implements DataOutputStream {

	private final DataStreamMeta dataStreamMeta;
	private final File file;

	public static FileDataOutputStream from(String filePath, boolean overwrite) throws IOException {
		return new FileDataOutputStream(filePath, overwrite);
	}

	public static FileDataOutputStream from(File file, boolean overwrite) throws IOException {
		return new FileDataOutputStream(file, overwrite);
	}

	public static FileDataOutputStream from(Path path, boolean overwrite) throws IOException {
		return new FileDataOutputStream(path, overwrite);
	}

	private FileDataOutputStream(Path path, boolean overwrite) throws IOException {
		this.dataStreamMeta = DataStreamMeta.fromPath(path);
		this.file = path.toFile();
		if (!overwrite && file.exists()) {
			throw new FileNotFoundException("File <" + file + "> exists and overwrite is false");
		}
		if (file.isDirectory()) {
			throw new IOException("Cannot write file.  File <" + file + "> is a directory");
		}
	}

	private FileDataOutputStream(File file, boolean overwrite) throws IOException {
		this.dataStreamMeta = DataStreamMeta.fromFile(file);
		this.file = file;
		if (!overwrite && file.exists()) {
			throw new FileNotFoundException("File <" + file + "> exists and overwrite is false");
		}
		if (file.isDirectory()) {
			throw new IOException("Cannot write file.  File <" + file + "> is a directory");
		}
	}

	private FileDataOutputStream(String filePath, boolean overwrite) throws IOException {
		this.dataStreamMeta = DataStreamMeta.fromFullPath(filePath);
		this.file = new File(filePath);
		if (!overwrite && this.file.exists()) {
			throw new FileNotFoundException("File <" + filePath + "> exists and overwrite is false");
		}
		if (this.file.isDirectory()) {
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
