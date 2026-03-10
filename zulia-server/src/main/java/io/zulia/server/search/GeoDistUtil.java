package io.zulia.server.search;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GeoDistUtil {

	private static final Pattern GEODIST_PATTERN = Pattern.compile("geodist\\(\\s*(\\w+)\\s*,\\s*(-?\\d+(?:\\.\\d+)?)\\s*,\\s*(-?\\d+(?:\\.\\d+)?)\\s*\\)");

	public record GeoDistParsed(String field, double latitude, double longitude) {
	}

	public static boolean isGeoDist(String input) {
		return input != null && input.startsWith("geodist(");
	}

	public static GeoDistParsed parseGeoDist(String input) {
		Matcher matcher = GEODIST_PATTERN.matcher(input);
		if (!matcher.matches()) {
			throw new IllegalArgumentException("Invalid geodist expression: " + input + ". Expected format: geodist(field, lat, lon)");
		}
		String field = matcher.group(1);
		double lat = Double.parseDouble(matcher.group(2));
		double lon = Double.parseDouble(matcher.group(3));
		return new GeoDistParsed(field, lat, lon);
	}

	public static Matcher findGeoDist(String input) {
		return GEODIST_PATTERN.matcher(input);
	}
}
