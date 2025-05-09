package io.zulia.ai.nn.training.config;

import ai.djl.training.evaluator.Evaluator;
import ai.djl.training.loss.L2Loss;
import ai.djl.training.loss.Loss;
import io.zulia.ai.nn.test.BinaryClassifierF1;
import io.zulia.ai.nn.test.BinaryClassifierPrecision;
import io.zulia.ai.nn.test.BinaryClassifierRecall;

import java.util.Collection;
import java.util.List;

public class DefaultBinarySettings extends DefaultTrainingSettings {

	private final float threshold;

	public DefaultBinarySettings(float threshold) {
		super();
		this.threshold = threshold;
	}

	@Override
	public Loss getLoss() {
		return new L2Loss();
	}

	@Override
	public Collection<Evaluator> getEvaluators() {
		return List.of(new BinaryClassifierPrecision(threshold) {
		}, new BinaryClassifierRecall(threshold), new BinaryClassifierF1(threshold));
	}

}
