package io.zulia.ai.nn.test;

import java.util.Objects;

public abstract class ClassifierStats<T> {
	protected int correctPredictions;
	protected int totalPredictions;

	public void merge(ClassifierStats<T> other) {
		this.correctPredictions += other.correctPredictions;
		this.totalPredictions += other.totalPredictions;
		mergeSpecific(other); // Downstream handling
	}

	public void evaluateResult(T predicted, T actual) {
		countPrediction(Objects.equals(predicted, actual));
		evaluateResultSpecific(predicted, actual); // Downstream handles its own stuff
	}

	public abstract float getF1();

	public abstract float getPrecision();

	public abstract float getRecall();

	public float getAccuracy() {
		return (float) correctPredictions / (float) totalPredictions;
	}

	protected abstract void mergeSpecific(ClassifierStats<T> other);

	protected abstract void evaluateResultSpecific(T predicted, T actual);

	protected void countPrediction(boolean matched) {
		correctPredictions += matched ? 1 : 0;
		totalPredictions++;
	}
}
