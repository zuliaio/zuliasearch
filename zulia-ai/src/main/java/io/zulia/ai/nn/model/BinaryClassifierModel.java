package io.zulia.ai.nn.model;

import ai.djl.MalformedModelException;
import ai.djl.Model;
import ai.djl.inference.Predictor;
import ai.djl.nn.Block;
import com.google.gson.Gson;
import io.zulia.ai.features.scaler.FeatureScaler;
import io.zulia.ai.features.stat.FeatureStat;
import io.zulia.ai.nn.config.FullyConnectedConfiguration;
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
import java.util.function.Function;

public class BinaryClassifierModel implements AutoCloseable {
	private final static Logger LOG = LoggerFactory.getLogger(BinaryClassifierModel.class);
	
	private final Model model;
	private final FeatureScaler featureScaler;
	
	public BinaryClassifierModel(String modelBaseDir, String modelUuid, String modelName, String modelSuffix,
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
	
	public Predictor<float[], Float> getPredictor() {
		return model.newPredictor(new ScaledFeatureBinaryOutputTranslator<>(featureScaler, new NoOpDenseFeatureGenerator()));
	}
	
	public <T> Predictor<T, Float> getPredictor(DenseFeatureGenerator<T> convertToDenseFeatures) {
		return model.newPredictor(new ScaledFeatureBinaryOutputTranslator<>(featureScaler, convertToDenseFeatures));
	}
	
	@Override
	public void close() {
		model.close();
	}
	
}
