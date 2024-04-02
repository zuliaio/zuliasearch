package io.zulia.testing;

import java.util.List;

public final class QueryConfig {
	private String index;
	private String query;

	private int amount;

	private List<FacetConfig> facets;

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

	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

	public List<FacetConfig> getFacets() {
		return facets;
	}

	public void setFacets(List<FacetConfig> facets) {
		this.facets = facets;
	}

	@Override
	public String toString() {
		return "QueryConfig{" + "index='" + index + '\'' + ", query='" + query + '\'' + ", amount=" + amount + ", facets=" + facets + '}';
	}
}
