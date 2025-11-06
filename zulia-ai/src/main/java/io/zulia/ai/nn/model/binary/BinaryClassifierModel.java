package io.zulia.ai.nn.model.binary;

import ai.djl.MalformedModelException;
import ai.djl.inference.Predictor;
import io.zulia.ai.features.scaler.FeatureScaler;
import io.zulia.ai.features.stat.FeatureStat;
import io.zulia.ai.nn.model.generics.ClassifierModel;
import io.zulia.ai.nn.translator.DenseFeatureGenerator;
import io.zulia.ai.nn.translator.NoOpDenseFeatureGenerator;
import io.zulia.ai.nn.translator.ScaledFeatureBinaryOutputTranslator;

import java.io.IOException;
import java.util.function.Function;

public class BinaryClassifierModel extends ClassifierModel<Float> {

	public BinaryClassifierModel(String modelBaseDir, String modelUuid, String modelName, String modelSuffix,
			Function<FeatureStat[], FeatureScaler> featureScalerGenerator) throws IOException, MalformedModelException {
		super(modelBaseDir, modelUuid, modelName, modelSuffix, featureScalerGenerator);

	}

	@Override
	public Predictor<float[], Float> getPredictor() {
		return getPredictor(1);
	}

	@Override
	public Predictor<float[], Float> getPredictor(int maxThreadsFeatureGenThreads) {
		return model.newPredictor(new ScaledFeatureBinaryOutputTranslator<>(maxThreadsFeatureGenThreads, featureScaler, new NoOpDenseFeatureGenerator()));
	}

	@Override
	public <T> Predictor<T, Float> getPredictor(DenseFeatureGenerator<T> convertToDenseFeatures) {
		return getPredictor(convertToDenseFeatures, 1);
	}

	@Override
	public <T> Predictor<T, Float> getPredictor(DenseFeatureGenerator<T> convertToDenseFeatures, int maxThreadsFeatureGenThreads) {
		return model.newPredictor(new ScaledFeatureBinaryOutputTranslator<>(maxThreadsFeatureGenThreads, featureScaler, convertToDenseFeatures));
	}
}
