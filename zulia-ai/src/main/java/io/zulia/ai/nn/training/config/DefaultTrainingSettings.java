package io.zulia.ai.nn.training.config;

import ai.djl.training.initializer.Initializer;
import ai.djl.training.initializer.XavierInitializer;
import ai.djl.training.listener.TrainingListener;
import ai.djl.training.optimizer.Adam;
import ai.djl.training.optimizer.Optimizer;
import ai.djl.training.tracker.ParameterTracker;
import ai.djl.training.tracker.Tracker;

import java.util.Collection;
import java.util.List;

public abstract class DefaultTrainingSettings implements TrainingSettings {

	private int batchSize = 1000;
	private int iterations = 8;
	private ParameterTracker learningRateTracker;
	private Float clipGrad;
	private Float weightDecays;
	private boolean trainInMemory = false;

	public DefaultTrainingSettings() {

	}

	@Override
	public int getIterations() {
		return iterations;
	}

	public DefaultTrainingSettings setIterations(int iterations) {
		this.iterations = iterations;
		return this;
	}

	@Override
	public int getBatchSize() {
		return batchSize;
	}

	public DefaultTrainingSettings setBatchSize(int batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	public DefaultTrainingSettings setFixedLearningRate(float learningRate) {
		this.learningRateTracker = Tracker.fixed(learningRate);
		return this;
	}

	public DefaultTrainingSettings setCosineLearningRate(float baseValue, float finalValue, int maxUpdates) {
		this.learningRateTracker = Tracker.cosine().setBaseValue(baseValue).optFinalValue(finalValue).setMaxUpdates(maxUpdates).build();
		return this;
	}

	public DefaultTrainingSettings setClipGrad(Float clipGrad) {
		this.clipGrad = clipGrad;
		return this;
	}

	public DefaultTrainingSettings setWeightDecays(Float weightDecays) {
		this.weightDecays = weightDecays;
		return this;
	}

	public DefaultTrainingSettings setTrainInMemory(boolean trainInMemory) {
		this.trainInMemory = trainInMemory;
		return this;
	}

	@Override
	public boolean getTrainInMemory() {
		return trainInMemory;
	}

	@Override
	public Optimizer getOptimizer() {
		Adam.Builder adam = Optimizer.adam();

		if (learningRateTracker != null) {
			adam.optLearningRateTracker(learningRateTracker);
		}
		if (clipGrad != null) {
			adam.optClipGrad(clipGrad);
		}
		if (weightDecays != null) {
			adam.optWeightDecays(weightDecays);
		}

		return adam.build();
	}

	public Collection<TrainingListener> getTrainingListeners() {
		return List.of(TrainingListener.Defaults.logging());
	}

	@Override
	public Initializer getWeightInitializer() {
		return new XavierInitializer();
	}
}
