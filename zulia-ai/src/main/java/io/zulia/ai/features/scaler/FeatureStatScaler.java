package io.zulia.ai.features.scaler;

import io.zulia.ai.features.stat.FeatureStat;

public abstract class FeatureStatScaler implements FeatureScaler {

	private final transient FeatureStat[] featureStats;
	private String description;

	public FeatureStatScaler(FeatureStat[] featureStats, String description) {
		this.featureStats = featureStats;
		this.description = description;
	}

	@Override
	public double[] scaleFeatures(double[] features) {
		if (features.length != featureStats.length) {
			throw new IllegalArgumentException("Features and feature stats arrays must have the same length");
		}

		double[] scaledFeatures = new double[features.length];
		for (int i = 0; i < features.length; i++) {
			scaledFeatures[i] = scaleFeature(featureStats[i], features[i]);
		}

		return scaledFeatures;
	}

	public float[] scaleFeatures(float[] features) {
		if (features.length != featureStats.length) {
			throw new IllegalArgumentException("Features and feature stats arrays must have the same length");
		}

		float[] scaledFeatures = new float[features.length];
		for (int i = 0; i < features.length; i++) {
			scaledFeatures[i] = scaleFeature(featureStats[i], features[i]);
		}

		return scaledFeatures;

	}

	protected abstract double scaleFeature(FeatureStat featureStat, double value);

	protected float scaleFeature(FeatureStat featureStat, float value) {
		return (float) scaleFeature(featureStat, (double) value);
	}

	@Override
	public String getDescription() {
		return description;
	}
}
