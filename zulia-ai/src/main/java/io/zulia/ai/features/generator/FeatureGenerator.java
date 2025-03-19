package io.zulia.ai.features.generator;

public interface FeatureGenerator<T> {
	
	double[] generateFeatures(T t);
	
	int getNumberOfFeatures();
}
