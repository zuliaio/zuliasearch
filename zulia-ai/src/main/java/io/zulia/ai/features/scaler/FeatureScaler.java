package io.zulia.ai.features.scaler;

public interface FeatureScaler {
	
	double[] scaleFeatures(double[] features);
	
	float[] scaleFeatures(float[] features);
}
