package io.zulia.data;

import java.io.File;
import java.net.URLConnection;

public record DataStreamMeta(String contentType, String fileName) {

	public static DataStreamMeta fromFileName(String fileName) {
		if (isGzipExtension(fileName)) {
			fileName = fileName.substring(0, fileName.length() - 3);
		}
		String contentType = URLConnection.guessContentTypeFromName(fileName);
		return new DataStreamMeta(contentType, fileName);
	}

	public static DataStreamMeta fromFullPath(String filePath) {
		File file = new File(filePath);
		String name = file.getName();

		String contentType = URLConnection.guessContentTypeFromName(name);
		return new DataStreamMeta(contentType, name);
	}

	public static boolean isGzipExtension(String fileName) {
		return fileName.toLowerCase().endsWith(".gz");
	}
}
