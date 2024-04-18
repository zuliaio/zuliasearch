package io.zulia.testing.config;

import java.util.List;

public class ZuliaTestConfig {

	private List<ConnectionConfig> connections;
	private List<IndexConfig> indexes;
	private List<SearchConfig> searches;
	private List<TestConfig> tests;

	private boolean logSearches;
	private boolean logSearchResults;

	public ZuliaTestConfig() {

	}

	public List<ConnectionConfig> getConnections() {
		return connections;
	}

	public void setConnections(List<ConnectionConfig> connections) {
		this.connections = connections;
	}

	public List<IndexConfig> getIndexes() {
		return indexes;
	}

	public void setIndexes(List<IndexConfig> indexes) {
		this.indexes = indexes;
	}

	public List<SearchConfig> getSearches() {
		return searches;
	}

	public void setSearches(List<SearchConfig> searches) {
		this.searches = searches;
	}

	public List<TestConfig> getTests() {
		return tests;
	}

	public void setTests(List<TestConfig> tests) {
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
