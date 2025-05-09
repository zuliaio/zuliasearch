package io.zulia.ai.nn.config;

import ai.djl.nn.Activation;
import ai.djl.nn.Block;
import ai.djl.nn.Parameter;
import ai.djl.nn.SequentialBlock;
import ai.djl.nn.core.Linear;
import ai.djl.nn.norm.BatchNorm;
import io.zulia.ai.nn.training.config.TrainingSettings;

import java.util.List;

public class FullyConnectedConfiguration implements NeuralNetworkConfiguration {

	private final List<Integer> hiddenSizes;
	private final int numberOfInputs;
	private final int numberOfOutputs;
	private ActivationFunction activation;
	private boolean sigmoidOutput;

	public FullyConnectedConfiguration(List<Integer> hiddenSizes, int numberOfInputs, int numberOfOutputs) {
		this.hiddenSizes = hiddenSizes;
		this.numberOfInputs = numberOfInputs;
		this.numberOfOutputs = numberOfOutputs;
		this.activation = ActivationFunction.Tanh;
	}

	public List<Integer> getHiddenSizes() {
		return hiddenSizes;
	}

	public int getNumberOfInputs() {
		return numberOfInputs;
	}

	public int getNumberOfOutputs() {
		return numberOfOutputs;
	}

	public FullyConnectedConfiguration setSigmoidOutput(boolean sigmoidOutput) {
		this.sigmoidOutput = sigmoidOutput;
		return this;
	}

	public boolean isSigmoidOutput() {
		return sigmoidOutput;
	}

	public FullyConnectedConfiguration setActivation(ActivationFunction activation) {
		this.activation = activation;
		return this;
	}

	public ActivationFunction getActivation() {
		return activation;
	}

	public boolean isBinary() {
		return numberOfOutputs == 1;
	}

	@Override
	public Block getTrainingNetwork(TrainingSettings trainingSettings) {
		// training network is the same except it needs weight initialization
		Block net = getEvaluationNetwork();
		if (trainingSettings.getWeightInitializer() != null) {
			net.setInitializer(trainingSettings.getWeightInitializer(), Parameter.Type.WEIGHT);
		}
		return net;
	}

	@Override
	public Block getEvaluationNetwork() {
		SequentialBlock net = new SequentialBlock();

		for (Integer layerSize : hiddenSizes) {
			net.add(Linear.builder().setUnits(layerSize).build());
			net.add(activation.getActivationFunction());
			net.add(BatchNorm.builder().build());
		}
		net.add(Linear.builder().setUnits(numberOfOutputs).build());
		if (sigmoidOutput) {
			net.add(Activation::sigmoid);
		}
		return net;
	}
}
