package io.zulia.ai.nn.model.multi;

import ai.djl.MalformedModelException;
import ai.djl.inference.Predictor;
import io.zulia.ai.features.scaler.FeatureScaler;
import io.zulia.ai.features.stat.FeatureStat;
import io.zulia.ai.nn.model.generics.ClassifierModel;
import io.zulia.ai.nn.translator.DenseFeatureGenerator;
import io.zulia.ai.nn.translator.NoOpDenseFeatureGenerator;
import io.zulia.ai.nn.translator.ScaledFeatureMultiOutputTranslator;

import java.io.IOException;
import java.util.function.Function;

public class MultiClassifierModel extends ClassifierModel<float[]> {
	public MultiClassifierModel(String modelBaseDir, String modelUuid, String modelName, String modelSuffix,
			Function<FeatureStat[], FeatureScaler> featureScalerGenerator) throws IOException, MalformedModelException {
		super(modelBaseDir, modelUuid, modelName, modelSuffix, featureScalerGenerator);
	}

	@Override
	public Predictor<float[], float[]> getPredictor() {
		return getPredictor(1);
	}

	@Override
	public Predictor<float[], float[]> getPredictor(int maxThreadsFeatureGenThreads) {
		return model.newPredictor(new ScaledFeatureMultiOutputTranslator<>(maxThreadsFeatureGenThreads, featureScaler, new NoOpDenseFeatureGenerator()));
	}

	@Override
	public <T> Predictor<T, float[]> getPredictor(DenseFeatureGenerator<T> convertToDenseFeatures) {
		return getPredictor(convertToDenseFeatures, 1);
	}

	@Override
	public <T> Predictor<T, float[]> getPredictor(DenseFeatureGenerator<T> convertToDenseFeatures, int maxThreadsFeatureGenThreads) {
		return model.newPredictor(new ScaledFeatureMultiOutputTranslator<>(maxThreadsFeatureGenThreads, featureScaler, convertToDenseFeatures));
	}
}
