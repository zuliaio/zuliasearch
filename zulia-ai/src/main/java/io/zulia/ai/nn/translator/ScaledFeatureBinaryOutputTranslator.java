package io.zulia.ai.nn.translator;

import ai.djl.ndarray.NDList;
import ai.djl.translate.Translator;
import ai.djl.translate.TranslatorContext;
import io.zulia.ai.features.scaler.FeatureScaler;

import java.util.function.Function;

public class ScaledFeatureBinaryOutputTranslator<T> implements Translator<T, Float> {
	
	private final FeatureScaler featureScaler;
	private final Function<T, float[]> convertToDenseFeatures;

	
	public ScaledFeatureBinaryOutputTranslator(FeatureScaler featureScaler, DenseFeatureGenerator<T> convertToDenseFeatures) {
		this.featureScaler = featureScaler;
		this.convertToDenseFeatures = convertToDenseFeatures;
	}
	
	@Override
	public Float processOutput(TranslatorContext ctx, NDList list) {
		return list.getFirst().getFloat(0);
	}
	
	@Override
	public NDList processInput(TranslatorContext ctx, T input) {
		float[] denseFeatures = convertToDenseFeatures.apply(input);
		float[] scaledFeatures = featureScaler.scaleFeatures(denseFeatures);
		return new NDList(ctx.getNDManager().create(scaledFeatures));
	}
}
