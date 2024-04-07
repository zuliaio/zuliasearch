package io.zulia.testing.config;

import java.util.List;

public class NumStatConfig {

	private String numericField;

	private double percentilePrecision;

	private List<Double> percentiles;

	public NumStatConfig() {
	}

	public String getNumericField() {
		return numericField;
	}

	public void setNumericField(String numericField) {
		this.numericField = numericField;
	}

	public double getPercentilePrecision() {
		return percentilePrecision;
	}

	public void setPercentilePrecision(double percentilePrecision) {
		this.percentilePrecision = percentilePrecision;
	}

	public List<Double> getPercentiles() {
		return percentiles;
	}

	public void setPercentiles(List<Double> percentiles) {
		this.percentiles = percentiles;
	}

	@Override
	public String toString() {
		return "NumStatConfig{" + "numericField='" + numericField + '\'' + ", percentilePrecision=" + percentilePrecision + ", percentiles=" + percentiles
				+ '}';
	}
}
