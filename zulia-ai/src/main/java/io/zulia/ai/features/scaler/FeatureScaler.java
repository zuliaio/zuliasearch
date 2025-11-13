package io.zulia.ai.features.scaler;

import java.io.Serializable;

public interface FeatureScaler extends Serializable {

	double[] scaleFeatures(double[] features);

	float[] scaleFeatures(float[] features);

	String getDescription();
}
