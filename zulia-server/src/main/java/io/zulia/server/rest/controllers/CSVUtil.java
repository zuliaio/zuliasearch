package io.zulia.server.rest.controllers;

public class CSVUtil {
	public static String quoteForCSV(String value) {
		if (value.contains(",") || value.contains(" ") || value.contains("\"") || value.contains("\n")) {
			return "\"" + value.replace("\"", "\"\"") + "\"";
		}
		else {
			return value;
		}
	}
}
