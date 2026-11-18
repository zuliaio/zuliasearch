package io.zulia.testing.result;

import io.zulia.testing.config.TestConfig;

public class TestResult {

	private String testId;

	private TestConfig testConfig;

	private TestStatus status = TestStatus.FAIL;

	private String message;

	public TestResult() {
	}

	public String getTestId() {
		return testId;
	}

	public void setTestId(String testId) {
		this.testId = testId;
	}

	public TestConfig getTestConfig() {
		return testConfig;
	}

	public void setTestConfig(TestConfig testConfig) {
		this.testConfig = testConfig;
	}

	public boolean isPassed() {
		return status == TestStatus.PASS;
	}

	public void setPassed(boolean passed) {
		this.status = passed ? TestStatus.PASS : TestStatus.FAIL;
	}

	public TestStatus getStatus() {
		return status;
	}

	public void setStatus(TestStatus status) {
		this.status = status;
	}

	/**
	 * Why the test is ERROR or SKIPPED, null for PASS and FAIL.
	 */
	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	@Override
	public String toString() {
		return "TestResult{" + "testId='" + testId + '\'' + ", testConfig=" + testConfig + ", status=" + status + ", message='" + message + '\'' + '}';
	}
}
