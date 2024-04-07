package io.zulia.testing.config;

import java.util.List;

public final class QueryConfig {
	private String index;
	private String query;

	private int amount;

	private List<FacetConfig> facets;

	private List<StatFacetConfig> statFacets;

	private List<NumStatConfig> numStats;

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

	public List<StatFacetConfig> getStatFacets() {
		return statFacets;
	}

	public void setStatFacets(List<StatFacetConfig> statFacets) {
		this.statFacets = statFacets;
	}

	public List<NumStatConfig> getNumStats() {
		return numStats;
	}

	public void setNumStats(List<NumStatConfig> numStats) {
		this.numStats = numStats;
	}

	@Override
	public String toString() {
		return "QueryConfig{" + "index='" + index + '\'' + ", query='" + query + '\'' + ", amount=" + amount + ", facets=" + facets + ", statFacets="
				+ statFacets + ", numStats=" + numStats + '}';
	}
}
