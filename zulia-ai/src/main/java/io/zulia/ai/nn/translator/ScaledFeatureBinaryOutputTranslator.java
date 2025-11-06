package io.zulia.ai.nn.translator;

import ai.djl.ndarray.NDList;
import ai.djl.translate.TranslatorContext;
import io.zulia.ai.features.scaler.FeatureScaler;

public class ScaledFeatureBinaryOutputTranslator<T> extends ScaledFeatureOutputTranslatorGeneric<T, Float> {
	public ScaledFeatureBinaryOutputTranslator(int maxThreads, FeatureScaler featureScaler, DenseFeatureGenerator<T> convertToDenseFeatures) {
		super(maxThreads, featureScaler, convertToDenseFeatures);
	}

	@Override
	public Float processOutput(TranslatorContext ctx, NDList list) {
		// Only thing unique is the output
		return list.getFirst().getFloat(0);
	}
}
