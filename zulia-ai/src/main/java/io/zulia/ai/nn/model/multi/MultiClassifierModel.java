package io.zulia.ai.nn.model.multi;

import ai.djl.MalformedModelException;
import ai.djl.inference.Predictor;
import ai.djl.translate.TranslateException;
import com.google.common.collect.Lists;
import io.zulia.ai.dataset.json.DenseFeatureAndCategoryDataset;
import io.zulia.ai.features.generator.ClassifierFeatureVector;
import io.zulia.ai.features.scaler.FeatureScaler;
import io.zulia.ai.features.stat.FeatureStat;
import io.zulia.ai.nn.model.generics.ClassifierModel;
import io.zulia.ai.nn.test.MultiClassifierStats;
import io.zulia.ai.nn.translator.DenseFeatureGenerator;
import io.zulia.ai.nn.translator.NoOpDenseFeatureGenerator;
import io.zulia.ai.nn.translator.ScaledFeatureMultiOutputTranslator;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

public class MultiClassifierModel extends ClassifierModel<float[]> {
	public MultiClassifierModel(String modelBaseDir, String modelUuid, String modelName, String modelSuffix,
			Function<FeatureStat[], FeatureScaler> featureScalerGenerator) throws IOException, MalformedModelException {
		super(modelBaseDir, modelUuid, modelName, modelSuffix, featureScalerGenerator);
	}

	/**
	 * Unique evaluator per model type
	 *
	 * @param dataset
	 * @param batchSize
	 * @return
	 * @throws TranslateException
	 */
	public MultiClassifierStats evaluate(DenseFeatureAndCategoryDataset dataset, int batchSize) throws TranslateException {
		List<List<ClassifierFeatureVector>> batches = Lists.partition(dataset.getClassifierFeatureVectors(), batchSize);

		MultiClassifierStats classifierStats = new MultiClassifierStats(dataset.getCategories());
		try (Predictor<float[], float[]> predictor = getPredictor()) {
			for (List<ClassifierFeatureVector> batch : batches) {
				ClassifierFeatureVector.BatchedClassifierFeatureVector batchedClassifierFeatureVector = ClassifierFeatureVector.asBatch(batch);
				List<float[]> outputs = predictor.batchPredict(batchedClassifierFeatureVector.featureList());
				for (int batchIndex = 0; batchIndex < outputs.size(); batchIndex++) {
					float[] output = outputs.get(batchIndex);
					float max = Float.MIN_VALUE;
					int predicted = -1; // Max value
					for (int i = 0; i < output.length; i++) {
						if (output[i] > max) {
							max = output[i];
							predicted = i;
						}
					}

					int actual = batchedClassifierFeatureVector.getCategory(batchIndex);
					classifierStats.evaluateResult(predicted, actual);
				}
			}
		}

		return classifierStats;

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
