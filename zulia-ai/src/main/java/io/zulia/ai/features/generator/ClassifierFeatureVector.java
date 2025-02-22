package io.zulia.ai.features.generator;

import java.util.ArrayList;
import java.util.List;

public class ClassifierFeatureVector extends TrainingFeatureVector {
	
	private int category;
	
	public ClassifierFeatureVector() {
		
	}
	
	public int getCategory() {
		return category;
	}
	
	public void setCategory(int category) {
		this.category = category;
	}
	
	public static BatchedClassifierFeatureVector asBatch(List<ClassifierFeatureVector> batch) {
		List<float[]> featureList = new ArrayList<>(batch.size());
		List<Integer> categories = new ArrayList<>(batch.size());
		for (ClassifierFeatureVector featureVector : batch) {
			featureList.add(featureVector.getFeatures());
			categories.add(featureVector.getCategory());
		}
		return new BatchedClassifierFeatureVector(featureList, categories);
	}
	
	public record BatchedClassifierFeatureVector(List<float[]> featureList, List<Integer> categories) {
		
		public int getCategory(int batchIndex) {
			return categories().get(batchIndex);
		}
	}
}
