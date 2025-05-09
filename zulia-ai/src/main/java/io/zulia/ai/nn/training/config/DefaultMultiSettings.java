package io.zulia.ai.nn.training.config;

import ai.djl.training.evaluator.Accuracy;
import ai.djl.training.evaluator.Evaluator;
import ai.djl.training.loss.Loss;
import ai.djl.training.loss.SoftmaxCrossEntropyLoss;

import java.util.Collection;
import java.util.List;

public class DefaultMultiSettings extends DefaultTrainingSettings {

	public DefaultMultiSettings() {

	}

	@Override
	public Loss getLoss() {
		return new SoftmaxCrossEntropyLoss("SoftmaxCrossEntropyLoss", 1, -1, false, true);
	}

	@Override
	public Collection<Evaluator> getEvaluators() {
		return List.of(new Accuracy());
	}

}
