package io.zulia.ai.features.scaler;

import io.zulia.ai.features.stat.FeatureStat;

public class MinMaxFeatureScaler extends FeatureStatScaler {

	public MinMaxFeatureScaler(FeatureStat[] featureStats) {
		super(featureStats, "MinMax");
	}

	@Override
	protected double scaleFeature(FeatureStat featureStat, double value) {
		double retVal = ((value - featureStat.getMin()) / (featureStat.getMax() - featureStat.getMin()));
		// a constant feature divides by zero, which is an infinity unless the numerator is zero too
		return Double.isFinite(retVal) ? retVal : 0;
	}

	@Override
	public String toString() {
		return "MinMaxFeatureScaler";
	}
}
