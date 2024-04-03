package io.zulia.testing.config;

public final class TestConfig {
	private String expr;

	public TestConfig() {

	}

	public String getExpr() {
		return expr;
	}

	public void setExpr(String expr) {
		this.expr = expr;
	}

	@Override
	public String toString() {
		return "TestConfig{" + "expr='" + expr + '\'' + '}';
	}
}
