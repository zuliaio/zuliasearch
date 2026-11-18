package io.zulia.testing.config;

/**
 * A named JavaScript expression whose result is recorded in the run report instead of asserted.
 */
public final class ValueConfig {

	private String name;

	private String expr;

	public ValueConfig() {

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
		return "ValueConfig{" + "name='" + name + '\'' + ", expr='" + expr + '\'' + '}';
	}
}
