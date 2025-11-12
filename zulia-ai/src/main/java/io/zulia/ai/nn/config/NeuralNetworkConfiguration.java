package io.zulia.ai.nn.config;

import ai.djl.nn.Block;
import io.zulia.ai.nn.training.config.TrainingSettings;

import java.io.Serializable;

public interface NeuralNetworkConfiguration extends Serializable {

	Block getTrainingNetwork(TrainingSettings trainingSettings);

	Block getEvaluationNetwork();
}
