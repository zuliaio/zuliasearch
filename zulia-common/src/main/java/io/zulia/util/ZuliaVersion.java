package io.zulia.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class ZuliaVersion {

	private static final ZuliaVersion VERSION = new ZuliaVersion();

	private final String version;

	public ZuliaVersion() {
		try (InputStream versionStream = ZuliaVersion.class.getResourceAsStream("/version")) {
			version = new String(versionStream.readAllBytes(), StandardCharsets.UTF_8);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	public static String getVersion() {
		return VERSION.version;
	}

	/**
	 * Gets the version of Zulia when versioning was added
	 * @return the version of Zulia when versioning was added
	 */
	public static String getVersionAdded() {
		return "2.1.0";
	}
}
