package io.zulia.util;

import java.util.Locale;

public class BooleanUtil {

	/**
	 * Parses the textual boolean forms Zulia accepts: true, t, yes, y, 1 and false, f, no, n, 0. Matching is case
	 * insensitive and surrounding whitespace is ignored. Returns null when the value is null, blank, or not one of
	 * the recognized forms.
	 */
	public static Boolean parseBoolean(String boolVal) {
		if (boolVal == null) {
			return null;
		}
		return switch (boolVal.trim().toLowerCase(Locale.ROOT)) {
			case "true", "t", "yes", "y", "1" -> Boolean.TRUE;
			case "false", "f", "no", "n", "0" -> Boolean.FALSE;
			default -> null;
		};
	}

	/**
	 * Parses a boolean from a Boolean, from a String using {@link #parseBoolean(String)}, or from a Number where 1 is
	 * true and 0 is false. Returns null for any other value or type, including null.
	 */
	public static Boolean parseBoolean(Object value) {
		return switch (value) {
			case Boolean booleanValue -> booleanValue;
			case String stringValue -> parseBoolean(stringValue);
			case Number numberValue -> parseBoolean(numberValue);
			case null, default -> null;
		};
	}

	/**
	 * Parses a boolean from a Number where 1 is true and 0 is false. Returns null for null or any other number.
	 */
	public static Boolean parseBoolean(Number number) {
		if (number == null) {
			return null;
		}
		double value = number.doubleValue();
		if (value == 1.0) {
			return Boolean.TRUE;
		}
		if (value == 0.0) {
			return Boolean.FALSE;
		}
		return null;
	}

	/**
	 * Returns 1 for a recognized true value, 0 for a recognized false value, and -1 for anything else.
	 */
	public static int getStringAsBooleanInt(String boolVal) {
		Boolean parsed = parseBoolean(boolVal);
		if (parsed == null) {
			return -1;
		}
		return parsed ? 1 : 0;
	}
}
