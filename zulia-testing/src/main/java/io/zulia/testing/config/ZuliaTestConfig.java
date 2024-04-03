package io.zulia.testing.config;

import java.util.Map;

public class ZuliaTestConfig {

	private Map<String, ConnectionConfig> connections;
	private Map<String, IndexConfig> indexes;
	private Map<String, QueryConfig> queries;
	private Map<String, TestConfig> tests;

	public ZuliaTestConfig() {

	}

	public Map<String, ConnectionConfig> getConnections() {
		return connections;
	}

	public void setConnections(Map<String, ConnectionConfig> connections) {
		this.connections = connections;
	}

	public Map<String, IndexConfig> getIndexes() {
		return indexes;
	}

	public void setIndexes(Map<String, IndexConfig> indexes) {
		this.indexes = indexes;
	}

	public Map<String, QueryConfig> getQueries() {
		return queries;
	}

	public void setQueries(Map<String, QueryConfig> queries) {
		this.queries = queries;
	}

	public Map<String, TestConfig> getTests() {
		return tests;
	}

	public void setTests(Map<String, TestConfig> tests) {
		this.tests = tests;
	}

	@Override
	public String toString() {
		return "DataQualityConfig{" + "connections=" + connections + ", indexes=" + indexes + ", queries=" + queries + ", tests=" + tests + '}';
	}
}
