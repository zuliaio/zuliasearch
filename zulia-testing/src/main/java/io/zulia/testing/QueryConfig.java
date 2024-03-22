package io.zulia.testing;

public final class QueryConfig {
	private String index;
	private String query;

	private int amount;

	public QueryConfig() {

	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	@Override
	public String toString() {
		return "QueryConfig[" + "index=" + index + ", " + "query=" + query + ']';
	}

}
