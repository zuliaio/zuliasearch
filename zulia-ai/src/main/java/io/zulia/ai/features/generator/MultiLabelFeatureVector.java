package io.zulia.ai.features.generator;

public class MultiLabelFeatureVector {

	private double[] features;
	private int[] labels;

	public MultiLabelFeatureVector() {

	}

	public double[] getFeatures() {
		return features;
	}

	public void setFeatures(double[] features) {
		this.features = features;
	}

	public int[] getLabels() {
		return labels;
	}

	public void setLabels(int[] labels) {
		this.labels = labels;
	}
}
