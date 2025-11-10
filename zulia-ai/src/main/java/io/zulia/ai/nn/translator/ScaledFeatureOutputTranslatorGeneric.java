package io.zulia.ai.nn.translator;

import ai.djl.ndarray.NDList;
import ai.djl.translate.Translator;
import ai.djl.translate.TranslatorContext;
import io.zulia.ai.features.scaler.FeatureScaler;
import io.zulia.util.pool.TaskExecutor;
import io.zulia.util.pool.ThreadedSequence;
import io.zulia.util.pool.WorkPool;

import java.util.ArrayList;
import java.util.List;

public abstract class ScaledFeatureOutputTranslatorGeneric<T, K> implements Translator<T, K> {

	private final FeatureScaler featureScaler;
	private final DenseFeatureGenerator<T> convertToDenseFeatures;
	private final int maxThreads;

	public ScaledFeatureOutputTranslatorGeneric(int maxThreads, FeatureScaler featureScaler, DenseFeatureGenerator<T> convertToDenseFeatures) {
		this.featureScaler = featureScaler;
		this.convertToDenseFeatures = convertToDenseFeatures;
		this.maxThreads = maxThreads;
	}

	@Override
	public NDList processInput(TranslatorContext ctx, T input) {
		float[] denseFeatures = convertToDenseFeatures.apply(input);
		float[] scaledFeatures = featureScaler.scaleFeatures(denseFeatures);
		return new NDList(ctx.getNDManager().create(scaledFeatures));
	}

	@Override
	public NDList batchProcessInput(TranslatorContext ctx, List<T> inputs) throws Exception {
		if (maxThreads > 1) {
			List<NDList> output = new ArrayList<>();
			try (TaskExecutor taskExecutor = WorkPool.virtualPool(maxThreads)) {
				ThreadedSequence<T, NDList> threadedSequence = new ThreadedSequence<>(taskExecutor, inputs.size()) {
					@Override
					public NDList doWork(T t) {
						return processInput(ctx, t);
					}

					@Override
					public void outputBatch(List<NDList> batchOut) {
						output.addAll(batchOut);
					}
				};
				threadedSequence.processThreaded(inputs);
				NDList[] processed = output.toArray(new NDList[0]);
				return getBatchifier().batchify(processed);
			}

		}
		else {
			return Translator.super.batchProcessInput(ctx, inputs);
		}
	}
}
