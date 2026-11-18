package io.zulia.testing.result;

/**
 * Outcome of one test, or of one value that could not be recorded.
 */
public enum TestStatus {
	PASS,
	FAIL,
	/**
	 * The expression could not be evaluated, or a test did not return a boolean.
	 */
	ERROR,
	/**
	 * The expression reads a saved value that no earlier run recorded, as in a baseline run.
	 */
	SKIPPED
}
