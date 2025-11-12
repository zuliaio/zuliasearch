package io.zulia.ai.nn.model.binary;

import ai.djl.MalformedModelException;
import ai.djl.inference.Predictor;
import ai.djl.translate.TranslateException;
import com.google.common.collect.Lists;
import io.zulia.ai.dataset.json.DenseFeatureAndCategoryDataset;
import io.zulia.ai.features.generator.ClassifierFeatureVector;
import io.zulia.ai.features.scaler.FeatureScaler;
import io.zulia.ai.features.stat.FeatureStat;
import io.zulia.ai.nn.model.generics.ClassifierModel;
import io.zulia.ai.nn.model.generics.ClassifierTrainingResults;
import io.zulia.ai.nn.test.BinaryClassifierStats;
import io.zulia.ai.nn.translator.DenseFeatureGenerator;
import io.zulia.ai.nn.translator.NoOpDenseFeatureGenerator;
import io.zulia.ai.nn.translator.ScaledFeatureBinaryOutputTranslator;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

public class BinaryClassifierModel extends ClassifierModel<Float> {

	public BinaryClassifierModel(String modelBaseDir, String modelUuid, String modelName, String modelSuffix,
			Function<FeatureStat[], FeatureScaler> featureScalerGenerator) throws IOException, MalformedModelException {
		super(modelBaseDir, modelUuid, modelName, modelSuffix, featureScalerGenerator);

	}

	public BinaryClassifierModel(Model model, FeatureScaler featureScaler) {
		super(model, featureScaler);
	}

	public BinaryClassifierModel(ClassifierTrainingResults results) {
		super(results.getResultModel(), results.getResultFeatureScaler());
	}

	public BinaryClassifierStats evaluate(DenseFeatureAndCategoryDataset dataset, float threshold, int batchSize) throws TranslateException {
		List<List<ClassifierFeatureVector>> batches = Lists.partition(dataset.getClassifierFeatureVectors(), batchSize);

		BinaryClassifierStats classifierStats = new BinaryClassifierStats();
		try (Predictor<float[], Float> predictor = getPredictor()) {
			for (List<ClassifierFeatureVector> batch : batches) {
				ClassifierFeatureVector.BatchedClassifierFeatureVector batchedClassifierFeatureVector = ClassifierFeatureVector.asBatch(batch);
				List<Float> outputs = predictor.batchPredict(batchedClassifierFeatureVector.featureList());
				for (int batchIndex = 0; batchIndex < outputs.size(); batchIndex++) {
					boolean predicted = outputs.get(batchIndex) >= threshold;
					boolean actual = batchedClassifierFeatureVector.getCategory(batchIndex) == 1;
					classifierStats.evaluateResult(predicted, actual);
				}
			}
		}

		return classifierStats;

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
