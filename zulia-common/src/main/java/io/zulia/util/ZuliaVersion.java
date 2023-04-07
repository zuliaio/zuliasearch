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

	private int major;
	private int minor;

	public ZuliaVersion() {
		try (InputStream versionStream = ZuliaVersion.class.getResourceAsStream("/version")) {
			version = new String(versionStream.readAllBytes(), StandardCharsets.UTF_8);

			String next = version;
			major = Integer.parseInt(next.substring(0, next.indexOf('.')));
			next = next.substring(next.indexOf('.') + 1);
			minor = Integer.parseInt(next.substring(0, next.indexOf('.')));
		}
		catch (IOException e) {
			LOG.log(Level.SEVERE, "Failed to get the version.", e);
		}

	}

	public static String getVersion() {
		return VERSION.version;
	}

	public static int getMajor() {
		return VERSION.major;
	}

	public static int getMinor() {
		return VERSION.minor;
	}

}
