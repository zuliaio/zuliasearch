package io.zulia.testing.result;

import io.zulia.testing.result.TestRunReport.Outcome;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

/**
 * Everything one test run produced. {@code unrecordedValues} lists the values that are missing from {@code values} because
 * their expression failed or was skipped.
 */
public record TestRun(Instant timestamp, LocalDate asOf, List<TestResult> testResults, Map<String, Object> values, List<Outcome> unrecordedValues) {

	public boolean anyFailed() {
		return testResults.stream().anyMatch(r -> r.getStatus() == TestStatus.FAIL);
	}

	/**
	 * True when a test or a value could not be evaluated, which points at the config and not at the data.
	 */
	public boolean anyErrors() {
		return testResults.stream().anyMatch(r -> r.getStatus() == TestStatus.ERROR) || unrecordedValues.stream().anyMatch(v -> v.status() == TestStatus.ERROR);
	}

	public TestRunReport toReport() {
		List<Outcome> outcomes = testResults.stream().map(r -> new Outcome(r.getTestId(), r.getStatus(), r.getMessage())).toList();
		return new TestRunReport(TestRunReport.CURRENT_FORMAT_VERSION, timestamp.toString(), asOf.toString(), values, outcomes, unrecordedValues);
	}
}
