package io.zulia.ai.nn.model.generics;

import ai.djl.MalformedModelException;
import ai.djl.Model;
import ai.djl.inference.Predictor;
import ai.djl.nn.Block;
import com.google.gson.Gson;
import io.zulia.ai.features.scaler.FeatureScaler;
import io.zulia.ai.features.stat.FeatureStat;
import io.zulia.ai.nn.config.FullyConnectedConfiguration;
import io.zulia.ai.nn.translator.DenseFeatureGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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

	public abstract Predictor<float[], K> getPredictor();

	public abstract Predictor<float[], K> getPredictor(int maxThreadsFeatureGenThreads);

	public abstract <T> Predictor<T, K> getPredictor(DenseFeatureGenerator<T> convertToDenseFeatures);

	public abstract <T> Predictor<T, K> getPredictor(DenseFeatureGenerator<T> convertToDenseFeatures, int maxThreadsFeatureGenThreads);

	@Override
	public void close() {
		model.close();
	}

}
