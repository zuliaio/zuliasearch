package io.zulia.ai.nn;

import io.zulia.ai.features.generator.ClassifierFeatureVector;

import java.util.List;

public interface FeatureProvider {
	
	List<ClassifierFeatureVector> getTrainingClassifierFeatureVectors();
	
	List<ClassifierFeatureVector> getTestingClassifierFeatureVectors();
}
