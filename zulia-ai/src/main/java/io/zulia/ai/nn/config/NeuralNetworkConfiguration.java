package io.zulia.ai.nn.config;

import ai.djl.nn.Block;
import io.zulia.ai.nn.training.config.TrainingSettings;

public interface NeuralNetworkConfiguration {

	Block getTrainingNetwork(TrainingSettings trainingSettings);

	Block getEvaluationNetwork();
}
