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
			accept(i, features[i]);
		}
	}

	public void addExample(float[] features) {
		if (features.length != numberOfFeatures) {
			throw new IllegalArgumentException("Number of features " + numberOfFeatures + " does not match features array size " + features.length);
		}
		for (int i = 0; i < numberOfFeatures; i++) {
			accept(i, features[i]);
		}
	}

	// NaN marks a missing feature and infinities cannot be sketched, so both are left out of the stats
	private void accept(int feature, double value) {
		if (Double.isFinite(value)) {
			featureSketches[feature].accept(value);
		}
	}

	public FeatureStat[] computeFeatureStats() {
		FeatureStat[] featureStats = new FeatureStat[numberOfFeatures];
		for (int i = 0; i < numberOfFeatures; i++) {
			QuantileSketch featureSketch = featureSketches[i];
			FeatureStat featureStat = new FeatureStat();
			if (featureSketch.isEmpty()) {
				featureStat.setAvg(Double.NaN);
				featureStat.setMin(Double.NaN);
				featureStat.setP05(Double.NaN);
				featureStat.setP10(Double.NaN);
				featureStat.setP25(Double.NaN);
				featureStat.setP50(Double.NaN);
				featureStat.setP75(Double.NaN);
				featureStat.setP90(Double.NaN);
				featureStat.setP95(Double.NaN);
				featureStat.setMax(Double.NaN);
				featureStats[i] = featureStat;
				continue;
			}
			double[] quantiles = featureSketch.quantiles(QUANTILES);
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
