package io.zulia.util;

import java.util.regex.Pattern;

public class BooleanUtil {
	private static final Pattern truePattern = Pattern.compile("true|t|yes|y|1", Pattern.CASE_INSENSITIVE);
	private static final Pattern falsePattern = Pattern.compile("false|f|no|n|0", Pattern.CASE_INSENSITIVE);

	public static int getStringAsBooleanInt(String boolVal) {
		if (truePattern.matcher(boolVal).matches()) {
			return 1;
		}
		else if (falsePattern.matcher(boolVal).matches()) {
			return 0;
		}
		return -1;
	}
}
