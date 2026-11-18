package io.zulia.testing.result;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.google.gson.ToNumberPolicy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * JSON record of one test run.  It contains when it ran, the date its templates were anchored to, every recorded value, the outcome of every test, and the
 * values that could not be recorded.
 * A later run can load the values back and compare against them.
 */
public record TestRunReport(int formatVersion, String timestamp, String asOf, Map<String, Object> values, List<Outcome> tests, List<Outcome> unrecordedValues) {

	/**
	 * A test, or a value that is missing from {@code values}. The message says why for ERROR and SKIPPED and is null otherwise.
	 */
	public record Outcome(String name, TestStatus status, String message) {
	}

	/**
	 * Written into every report, so a later reader can tell which layout it is loading.
	 */
	public static final int CURRENT_FORMAT_VERSION = 1;

	private static final Gson GSON = new GsonBuilder().setPrettyPrinting().serializeNulls().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

	public void write(Path path) throws IOException {
		Files.writeString(path, GSON.toJson(this));
	}

	public static TestRunReport read(Path path) throws IOException {
		TestRunReport report;
		try {
			report = GSON.fromJson(Files.readString(path), TestRunReport.class);
		}
		catch (JsonParseException e) {
			throw new IOException("Run report <" + path + "> is not valid JSON: " + e.getMessage(), e);
		}
		if (report == null || report.values() == null) {
			throw new IOException("Run report <" + path + "> does not contain a values section");
		}
		return report;
	}

	public String toJson() {
		return GSON.toJson(this);
	}
}
