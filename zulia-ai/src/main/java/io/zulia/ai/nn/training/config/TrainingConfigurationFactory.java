package io.zulia.ai.nn.training.config;

import ai.djl.training.DefaultTrainingConfig;
import ai.djl.training.evaluator.Evaluator;
import ai.djl.training.listener.TrainingListener;

public class TrainingConfigurationFactory {
	
	public static DefaultTrainingConfig getTrainingConfig(TrainingSettings trainingSettings) {
		DefaultTrainingConfig defaultTrainingConfig = new DefaultTrainingConfig(trainingSettings.getLoss());
		defaultTrainingConfig.optOptimizer(trainingSettings.getOptimizer());
		for (Evaluator evaluator : trainingSettings.getEvaluators()) {
			defaultTrainingConfig.addEvaluator(evaluator);
		}
		if (trainingSettings.getTrainingListeners() != null) {
			for (TrainingListener trainingListener : trainingSettings.getTrainingListeners()) {
				defaultTrainingConfig.addTrainingListeners(trainingListener);
			}
		}
		
		return defaultTrainingConfig;
	}
}
