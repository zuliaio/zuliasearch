package io.zulia.testing;

public class FacetConfig {

	private String field;
	private int topN;

	public FacetConfig() {
	}

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public int getTopN() {
		return topN;
	}

	public void setTopN(int topN) {
		this.topN = topN;
	}

	@Override
	public String toString() {
		return "FacetConfig{" + "field='" + field + '\'' + ", topN=" + topN + '}';
	}
}
