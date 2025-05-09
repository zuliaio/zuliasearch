package io.zulia.util;

import com.google.common.base.Joiner;

import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Created by mdavis on 3/27/16.
 */
public class QueryHelper {

	public static final Joiner COMMA_JOINER = Joiner.on(",").skipNulls();
	public static final Joiner SPACE_JOINER = Joiner.on(" ").skipNulls();
	public static final Joiner OR_JOINER = Joiner.on(" OR ").skipNulls();
	public static final Joiner AND_JOINER = Joiner.on(" AND ").skipNulls();

	public static final Pattern NEEDS_QUOTING = Pattern.compile("[\\s\\\\+\\-!():^\\[\\]\"{}~*?|&/@]");

	public final static Function<String, String> VALUE_QUOTER = s -> {
		s = s.trim();
		//if starts and ends with quotes pass it through
		if (s.startsWith("\"") && s.endsWith("\"")) {
			return s;
		}

		//if it contains a space, quote and pass it through after escaping quotes
		if (NEEDS_QUOTING.matcher(s).find()) {
			return "\"" + escapeQuotes(s) + "\"";
		}

		return s;

	};

	public static String escapeQuotes(String s) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			// These characters are part of the query syntax and must be escaped
			if (c == '\"') {
				sb.append('\\');
			}
			sb.append(c);
		}
		return sb.toString();
	}

	//taken from Lucene QueryParserUtil with @ added
	public static String escape(String s) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			// These characters are part of the query syntax and must be escaped
			if (c == '\\' || c == '+' || c == '-' || c == '!' || c == '(' || c == ')' || c == ':' || c == '^' || c == '[' || c == ']' || c == '\"' || c == '{'
					|| c == '}' || c == '~' || c == '*' || c == '?' || c == '|' || c == '&' || c == '/' || c == '@') {
				sb.append('\\');
			}
			sb.append(c);
		}
		return sb.toString();
	}

}
