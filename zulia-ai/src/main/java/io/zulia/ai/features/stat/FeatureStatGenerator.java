package io.zulia.ai.features.stat;

import io.zulia.util.sketch.QuantileSketch;

public class FeatureStatGenerator {

	private static final double[] QUANTILES = { 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95 };

	private final int numberOfFeatures;
	private final QuantileSketch[] featureSketches;

	public FeatureStatGenerator(int numberOfFeatures) {
		this.numberOfFeatures = numberOfFeatures;
		this.featureSketches = new QuantileSketch[numberOfFeatures];
		for (int i = 0; i < numberOfFeatures; i++) {
			featureSketches[i] = new QuantileSketch(0.0001, QuantileSketch.Mapping.CUBIC);
		}
	}

	public void addExample(double[] features) {
		if (features.length != numberOfFeatures) {
			throw new IllegalArgumentException("Number of features " + numberOfFeatures + " does not match features array size " + features.length);
		}
		for (int i = 0; i < numberOfFeatures; i++) {
			featureSketches[i].accept(features[i]);
		}
	}

	public void addExample(float[] features) {
		if (features.length != numberOfFeatures) {
			throw new IllegalArgumentException("Number of features " + numberOfFeatures + " does not match features array size " + features.length);
		}
		for (int i = 0; i < numberOfFeatures; i++) {
			featureSketches[i].accept(features[i]);
		}
	}

	public FeatureStat[] computeFeatureStats() {
		FeatureStat[] featureStats = new FeatureStat[numberOfFeatures];
		for (int i = 0; i < numberOfFeatures; i++) {
			QuantileSketch featureSketch = featureSketches[i];
			double[] quantiles = featureSketch.quantiles(QUANTILES);
			FeatureStat featureStat = new FeatureStat();
			featureStat.setAvg(featureSketch.average());
			featureStat.setMin(featureSketch.min());
			featureStat.setP05(quantiles[0]);
			featureStat.setP10(quantiles[1]);
			featureStat.setP25(quantiles[2]);
			featureStat.setP50(quantiles[3]);
			featureStat.setP75(quantiles[4]);
			featureStat.setP90(quantiles[5]);
			featureStat.setP95(quantiles[6]);
			featureStat.setMax(featureSketch.max());
			featureStats[i] = featureStat;
		}
		return featureStats;
	}

}
