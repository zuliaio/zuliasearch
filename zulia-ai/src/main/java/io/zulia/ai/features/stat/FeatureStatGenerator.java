package io.zulia.ai.features.stat;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.DDSketches;

public class FeatureStatGenerator {

	private final int numberOfFeatures;
	private final DDSketch[] featureSketches;

	public FeatureStatGenerator(int numberOfFeatures) {
		this.numberOfFeatures = numberOfFeatures;
		this.featureSketches = new DDSketch[numberOfFeatures];
		for (int i = 0; i < numberOfFeatures; i++) {
			featureSketches[i] = DDSketches.unboundedDense(0.0001);
		}
	}

	public void addExample(double[] features) {
		if (features.length != numberOfFeatures) {
			throw new IllegalArgumentException("Number of features <" + numberOfFeatures + "> does not match features array size <" + features.length + ">");
		}
		for (int i = 0; i < numberOfFeatures; i++) {
			featureSketches[i].accept(features[i]);
		}
	}

	public void addExample(float[] features) {
		if (features.length != numberOfFeatures) {
			throw new IllegalArgumentException("Number of features <" + numberOfFeatures + "> does not match features array size <" + features.length + ">");
		}
		for (int i = 0; i < numberOfFeatures; i++) {
			featureSketches[i].accept(features[i]);
		}
	}

	public FeatureStat[] computeFeatureStats() {
		FeatureStat[] featureStats = new FeatureStat[numberOfFeatures];
		for (int i = 0; i < numberOfFeatures; i++) {
			DDSketch featureSketch = featureSketches[i];
			FeatureStat featureStat = new FeatureStat();
			featureStat.setAvg(featureSketch.getAverage());
			featureStat.setMin(featureSketch.getMinValue());
			featureStat.setP05(featureSketch.getValueAtQuantile(0.05));
			featureStat.setP10(featureSketch.getValueAtQuantile(0.10));
			featureStat.setP25(featureSketch.getValueAtQuantile(0.25));
			featureStat.setP50(featureSketch.getValueAtQuantile(0.50));
			featureStat.setP75(featureSketch.getValueAtQuantile(0.75));
			featureStat.setP90(featureSketch.getValueAtQuantile(0.90));
			featureStat.setP95(featureSketch.getValueAtQuantile(0.95));
			featureStat.setMax(featureSketch.getMaxValue());
			featureStats[i] = featureStat;
		}
		return featureStats;
	}

}
