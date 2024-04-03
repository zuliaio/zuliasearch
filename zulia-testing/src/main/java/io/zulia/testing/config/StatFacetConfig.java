package io.zulia.testing.config;

public class StatFacetConfig {

	private String facetField;

	private String numericField;
	private int topN;

	public StatFacetConfig() {
	}

	public String getFacetField() {
		return facetField;
	}

	public void setFacetField(String facetField) {
		this.facetField = facetField;
	}

	public String getNumericField() {
		return numericField;
	}

	public void setNumericField(String numericField) {
		this.numericField = numericField;
	}

	public int getTopN() {
		return topN;
	}

	public void setTopN(int topN) {
		this.topN = topN;
	}

	@Override
	public String toString() {
		return "StatFacetConfig{" + "facetField='" + facetField + '\'' + ", numericField='" + numericField + '\'' + ", topN=" + topN + '}';
	}
}
