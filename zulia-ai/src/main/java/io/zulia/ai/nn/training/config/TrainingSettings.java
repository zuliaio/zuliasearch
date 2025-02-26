package io.zulia.ai.nn.training.config;

import ai.djl.training.evaluator.Evaluator;
import ai.djl.training.initializer.Initializer;
import ai.djl.training.listener.TrainingListener;
import ai.djl.training.loss.Loss;
import ai.djl.training.optimizer.Optimizer;

import java.util.Collection;

public interface TrainingSettings {
	
	Loss getLoss();
	
	Optimizer getOptimizer();
	
	Collection<Evaluator> getEvaluators();
	
	Collection<TrainingListener> getTrainingListeners();
	
	Initializer getWeightInitializer();
	
	int getBatchSize();
	
	int getIterations();
	
}
