package io.zulia.testing.config;

import java.util.List;

public final class SearchConfig {
	private String name;

	private String index;
	private List<QueryConfig> queries;

	private int amount;

	private List<String> documentFields;

	private List<FacetConfig> facets;

	private List<StatFacetConfig> statFacets;

	private List<NumStatConfig> numStats;

	public SearchConfig() {

	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public List<QueryConfig> getQueries() {
		return queries;
	}

	public void setQueries(List<QueryConfig> queries) {
		this.queries = queries;
	}

	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

	public List<String> getDocumentFields() {
		return documentFields;
	}

	public void setDocumentFields(List<String> documentFields) {
		this.documentFields = documentFields;
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
		return "SearchConfig{" + "name='" + name + '\'' + ", index='" + index + '\'' + ", queries=" + queries + ", amount=" + amount + ", documentFields="
				+ documentFields + ", facets=" + facets + ", statFacets=" + statFacets + ", numStats=" + numStats + '}';
	}
}
