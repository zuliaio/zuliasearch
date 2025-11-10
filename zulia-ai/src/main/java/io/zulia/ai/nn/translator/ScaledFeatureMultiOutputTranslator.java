package io.zulia.ai.nn.translator;

import ai.djl.ndarray.NDList;
import ai.djl.translate.TranslatorContext;
import io.zulia.ai.features.scaler.FeatureScaler;

public class ScaledFeatureMultiOutputTranslator<T> extends ScaledFeatureOutputTranslatorGeneric<T, float[]> {

	public ScaledFeatureMultiOutputTranslator(int maxThreads, FeatureScaler featureScaler, DenseFeatureGenerator<T> convertToDenseFeatures) {
		super(maxThreads, featureScaler, convertToDenseFeatures);
	}

	@Override
	public float[] processOutput(TranslatorContext ctx, NDList list) {
		// Only thing unique is the output
		return list.getFirst().toFloatArray();
	}

}
