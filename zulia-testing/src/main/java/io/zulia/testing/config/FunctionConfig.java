package io.zulia.testing.config;

import java.util.List;

/**
 * A JavaScript function defined once and callable from every test and value expression. Give either a single {@code expr} or a statement {@code body}.
 */
public final class FunctionConfig {

	private String name;

	private List<String> args;

	private String expr;

	private String body;

	public FunctionConfig() {

	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<String> getArgs() {
		return args;
	}

	public void setArgs(List<String> args) {
		this.args = args;
	}

	public String getExpr() {
		return expr;
	}

	public void setExpr(String expr) {
		this.expr = expr;
	}

	public String getBody() {
		return body;
	}

	public void setBody(String body) {
		this.body = body;
	}

	@Override
	public String toString() {
		return "FunctionConfig{" + "name='" + name + '\'' + ", args=" + args + ", expr='" + expr + '\'' + ", body='" + body + '\'' + '}';
	}
}
