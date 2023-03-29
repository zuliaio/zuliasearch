package io.zulia.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ZuliaVersion {

	private static final Logger LOG = Logger.getLogger(ZuliaVersion.class.getSimpleName());
	private static final ZuliaVersion VERSION = new ZuliaVersion();
	private String version;

	public ZuliaVersion() {
		try (InputStream versionStream = ZuliaVersion.class.getResourceAsStream("/version")) {
			version = new String(versionStream.readAllBytes(), StandardCharsets.UTF_8);
		}
		catch (IOException e) {
			LOG.log(Level.SEVERE, "Failed to get the version.", e);
		}

	}

	public static String getVersion() {
		return VERSION.version;
	}

	/**
	 * Gets the version of Zulia when versioning was added
	 *
	 * @return the version of Zulia when versioning was added
	 */
	public static String getVersionAdded() {
		return "2.1.0";
	}
}
