package io.zulia.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Class for removal of a (normal temp) file after it is done being read
 */
public class TempFileDeletingInputStream extends FileInputStream {

	private final File file;

	/**
	 * @param file - file to delete on stream close
	 * @throws FileNotFoundException - if file does not exist
	 */
	public TempFileDeletingInputStream(File file) throws FileNotFoundException {
		super(file);
		this.file = file;
	}

	@Override
	public void close() throws IOException {
		super.close();
		file.delete();
	}
}
