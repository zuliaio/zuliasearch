package io.zulia.testing.config;

import java.util.Map;

public class ZuliaTestConfig {

	private Map<String, ConnectionConfig> connections;
	private Map<String, IndexConfig> indexes;
	private Map<String, QueryConfig> searches;
	private Map<String, TestConfig> tests;

	private boolean logSearches;
	private boolean logSearchResults;

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

	public Map<String, QueryConfig> getSearches() {
		return searches;
	}

	public void setSearches(Map<String, QueryConfig> searches) {
		this.searches = searches;
	}

	public Map<String, TestConfig> getTests() {
		return tests;
	}

	public void setTests(Map<String, TestConfig> tests) {
		this.tests = tests;
	}

	public boolean isLogSearches() {
		return logSearches;
	}

	public void setLogSearches(boolean logSearches) {
		this.logSearches = logSearches;
	}

	public boolean isLogSearchResults() {
		return logSearchResults;
	}

	public void setLogSearchResults(boolean logSearchResults) {
		this.logSearchResults = logSearchResults;
	}

	@Override
	public String toString() {
		return "ZuliaTestConfig{" + "connections=" + connections + ", indexes=" + indexes + ", searches=" + searches + ", tests=" + tests + ", logSearches="
				+ logSearches + ", logSearchResults=" + logSearchResults + '}';
	}
}
