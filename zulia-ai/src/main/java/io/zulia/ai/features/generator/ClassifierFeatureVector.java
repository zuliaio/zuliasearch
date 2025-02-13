package io.zulia.ai.features.generator;

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
}
