package io.zulia.ai.nn.config;

import ai.djl.ndarray.NDList;
import ai.djl.nn.Activation;

import java.util.function.Function;

public enum ActivationFunction {
	Relu(Activation::relu),
	Tanh(Activation::tanh),
	LRelu((list) -> Activation.leakyRelu(list, 0.01f)),
	Sigmoid(Activation::sigmoid);
	
	private final Function<NDList, NDList> activationFunction;
	
	ActivationFunction(Function<NDList, NDList> activationFunction) {
		this.activationFunction = activationFunction;
	}
	
	public Function<NDList, NDList> getActivationFunction() {
		return activationFunction;
	}
}
