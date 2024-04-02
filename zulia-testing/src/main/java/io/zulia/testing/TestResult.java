package io.zulia.testing;

public class TestResult {

	private String testId;

	private TestConfig testConfig;

	private boolean passed;

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
		return passed;
	}

	public void setPassed(boolean passed) {
		this.passed = passed;
	}

	@Override
	public String toString() {
		return "TestResult{" + "testId='" + testId + '\'' + ", testConfig=" + testConfig + ", passed=" + passed + '}';
	}
}
