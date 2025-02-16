package io.zulia.ai.nn.translator;

import ai.djl.ndarray.NDList;
import ai.djl.translate.Translator;
import ai.djl.translate.TranslatorContext;
import io.zulia.ai.features.scaler.FeatureScaler;

public class ScaledFeatureBinaryOutputTranslator implements Translator<float[], Float> {
	
	private final FeatureScaler featureScaler;
	
	public ScaledFeatureBinaryOutputTranslator(FeatureScaler featureScaler) {
		this.featureScaler = featureScaler;
	}
	
	@Override
	public Float processOutput(TranslatorContext ctx, NDList list) {
		return list.getFirst().getFloat(0);
	}
	
	@Override
	public NDList processInput(TranslatorContext ctx, float[] input) {
		float[] scaledFeatures = featureScaler.scaleFeatures(input);
		return new NDList(ctx.getNDManager().create(scaledFeatures));
	}
}
