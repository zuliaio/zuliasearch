package io.zulia.ai.nn.model.generics;

import ai.djl.MalformedModelException;
import ai.djl.Model;
import ai.djl.inference.Predictor;
import ai.djl.nn.Block;
import ai.djl.translate.TranslateException;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import io.zulia.ai.dataset.json.DenseFeatureAndCategoryDataset;
import io.zulia.ai.features.generator.ClassifierFeatureVector;
import io.zulia.ai.features.generator.ClassifierFeatureVector.BatchedClassifierFeatureVector;
import io.zulia.ai.features.scaler.FeatureScaler;
import io.zulia.ai.features.stat.FeatureStat;
import io.zulia.ai.nn.config.FullyConnectedConfiguration;
import io.zulia.ai.nn.test.BinaryClassifierStats;
import io.zulia.ai.nn.translator.DenseFeatureGenerator;
import io.zulia.ai.nn.translator.NoOpDenseFeatureGenerator;
import io.zulia.ai.nn.translator.ScaledFeatureBinaryOutputTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;

public abstract class ClassifierModel<K> implements AutoCloseable {
	private final static Logger LOG = LoggerFactory.getLogger(ClassifierModel.class);

	protected final Model model;
	protected final FeatureScaler featureScaler;

	public ClassifierModel(String modelBaseDir, String modelUuid, String modelName, String modelSuffix,
			Function<FeatureStat[], FeatureScaler> featureScalerGenerator) throws IOException, MalformedModelException {
		model = Model.newInstance(modelName);

		Gson gson = new Gson();

		Path modelPath = Path.of(modelBaseDir, modelUuid);
		String paramsName = modelName + "_" + modelSuffix;
		Path featureStatPath = modelPath.resolve(modelName + "_" + "feature_stats.json");
		FeatureStat[] featureStats = gson.fromJson(Files.readString(featureStatPath), FeatureStat[].class);
		featureScaler = featureScalerGenerator.apply(featureStats);

		Path fullConfigPath = modelPath.resolve(modelName + "_" + "full_network_config.json");
		FullyConnectedConfiguration fullyConnectedConfiguration = gson.fromJson(Files.readString(fullConfigPath), FullyConnectedConfiguration.class);

		Block evaluationNetwork = fullyConnectedConfiguration.getEvaluationNetwork();
		try (DataInputStream is = new DataInputStream(new FileInputStream(modelPath.resolve(paramsName).toFile()))) {
			evaluationNetwork.loadParameters(model.getNDManager(), is);
		}
		model.setBlock(evaluationNetwork);
	}

	public BinaryClassifierStats evaluate(DenseFeatureAndCategoryDataset dataset, float threshold, int batchSize) throws TranslateException {

		List<List<ClassifierFeatureVector>> batches = Lists.partition(dataset.getClassifierFeatureVectors(), batchSize);

		BinaryClassifierStats binaryClassifierStats = new BinaryClassifierStats();
		try (Predictor<float[], Float> predictor = getPredictor()) {
			for (List<ClassifierFeatureVector> batch : batches) {
				BatchedClassifierFeatureVector batchedClassifierFeatureVector = ClassifierFeatureVector.asBatch(batch);
				List<Float> outputs = predictor.batchPredict(batchedClassifierFeatureVector.featureList());
				for (int batchIndex = 0; batchIndex < outputs.size(); batchIndex++) {
					boolean predicted = outputs.get(batchIndex) >= threshold;
					boolean actual = batchedClassifierFeatureVector.getCategory(batchIndex) == 1;
					binaryClassifierStats.evaluateResult(predicted, actual);
				}
			}
		}

		return binaryClassifierStats;

	}

	public Predictor<float[], K> getPredictor() {
		return getPredictor(1);
	}

	public Predictor<float[], K> getPredictor(int maxThreadsFeatureGenThreads) {
		return model.newPredictor(new ScaledFeatureBinaryOutputTranslator<>(maxThreadsFeatureGenThreads, featureScaler, new NoOpDenseFeatureGenerator()));
	}

	public <T> Predictor<T, K> getPredictor(DenseFeatureGenerator<T> convertToDenseFeatures) {
		return getPredictor(convertToDenseFeatures, 1);
	}

	public <T> Predictor<T, K> getPredictor(DenseFeatureGenerator<T> convertToDenseFeatures, int maxThreadsFeatureGenThreads) {
		return model.newPredictor(new ScaledFeatureBinaryOutputTranslator<>(maxThreadsFeatureGenThreads, featureScaler, convertToDenseFeatures));
	}

	@Override
	public void close() {
		model.close();
	}

}
