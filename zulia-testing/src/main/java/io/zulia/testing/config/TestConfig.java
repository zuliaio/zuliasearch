package io.zulia.testing.config;

public final class TestConfig {

	private String name;

	private String expr;

	public TestConfig() {

	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getExpr() {
		return expr;
	}

	public void setExpr(String expr) {
		this.expr = expr;
	}

	@Override
	public String toString() {
		return "TestConfig{" + "name='" + name + '\'' + ", expr='" + expr + '\'' + '}';
	}
}
