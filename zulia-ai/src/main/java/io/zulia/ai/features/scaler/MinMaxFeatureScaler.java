package io.zulia.ai.features.scaler;

import io.zulia.ai.features.stat.FeatureStat;

public class MinMaxFeatureScaler extends FeatureStatScaler {
	
	public MinMaxFeatureScaler(FeatureStat[] featureStats) {
		super(featureStats);
	}
	
	@Override
	protected double scaleFeature(FeatureStat featureStat, double value) {
		double retVal = ((value - featureStat.getMin()) / (featureStat.getMax() - featureStat.getMin()));
		if (Double.isNaN(retVal)) {
			retVal = 0;
		}
		return retVal;
	}
}
