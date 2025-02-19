package io.zulia.ai.nn.model;

import ai.djl.Model;
import ai.djl.metric.Metrics;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Block;
import ai.djl.training.DefaultTrainingConfig;
import ai.djl.training.EasyTrain;
import ai.djl.training.Trainer;
import ai.djl.training.dataset.Batch;
import ai.djl.translate.TranslateException;
import com.google.gson.Gson;
import io.zulia.ai.dataset.DenseFeatureAndCategoryDataset;
import io.zulia.ai.features.scaler.FeatureScaler;
import io.zulia.ai.features.stat.FeatureStat;
import io.zulia.ai.nn.config.FullyConnectedConfiguration;
import io.zulia.ai.nn.training.config.TrainingConfigurationFactory;
import io.zulia.ai.nn.training.config.TrainingSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.function.Function;

public class BinaryClassifierTrainer {
	public static final String FEATURE_STATS_JSON = "feature_stats.json";
	public static final String FULL_NETWORK_CONFIG_JSON = "full_network_config.json";
	
	private final static Logger LOG = LoggerFactory.getLogger(BinaryClassifierTrainer.class);
	
	private final String modelBaseDir;
	private final String modelName;
	private final FullyConnectedConfiguration fullyConnectedConfiguration;
	private final TrainingSettings trainingSettings;
	private final Function<FeatureStat[], FeatureScaler> featureScalerGenerator;
	
	public BinaryClassifierTrainer(String modelBaseDir, String modelName, FullyConnectedConfiguration fullyConnectedConfiguration,
					TrainingSettings trainingSettings, Function<FeatureStat[], FeatureScaler> featureScalerGenerator) {
		this.modelBaseDir = modelBaseDir;
		this.modelName = modelName;
		this.fullyConnectedConfiguration = fullyConnectedConfiguration;
		this.trainingSettings = trainingSettings;
		this.featureScalerGenerator = featureScalerGenerator;
	}
	
	public String getModelBaseDir() {
		return modelBaseDir;
	}
	
	public String getModelName() {
		return modelName;
	}
	
	public FullyConnectedConfiguration getFullyConnectedConfiguration() {
		return fullyConnectedConfiguration;
	}
	
	public TrainingSettings getTrainingSettings() {
		return trainingSettings;
	}
	
	public BinaryClassifierTrainingResults train(DenseFeatureAndCategoryDataset trainingSet, DenseFeatureAndCategoryDataset testingSet)
					throws IOException, TranslateException {
		
		FeatureScaler featureScaler = featureScalerGenerator.apply(trainingSet.getFeatureStats());
		trainingSet.setFeatureScaler(featureScaler);
		testingSet.setFeatureScaler(featureScaler);
		
		String modelUuid = UUID.randomUUID().toString();
		Path modelPath = Path.of(modelBaseDir, modelUuid);
		Files.createDirectory(modelPath);
		LOG.info("Model directory: {}", modelPath);
		
		Gson gson = new Gson();
		
		Model model = Model.newInstance(modelName);
		Block trainingNetwork = fullyConnectedConfiguration.getTrainingNetwork(trainingSettings);
		model.setBlock(trainingNetwork);
		
		DefaultTrainingConfig trainingConfig = TrainingConfigurationFactory.getTrainingConfig(trainingSettings);
		
		Trainer trainer = model.newTrainer(trainingConfig);
		trainer.initialize(new Shape(trainingSettings.getBatchSize(), fullyConnectedConfiguration.getNumberOfInputs()));
		
		Metrics metrics = new Metrics();
		trainer.setMetrics(metrics);
		
		String featureStatJson = gson.toJson(trainingSet.getFeatureStats());
		Files.writeString(modelPath.resolve(model.getName() + "_" + FEATURE_STATS_JSON), featureStatJson);
		
		String fullConnectionNetworkConfigJson = gson.toJson(fullyConnectedConfiguration);
		Files.writeString(modelPath.resolve(model.getName() + "_" + FULL_NETWORK_CONFIG_JSON), fullConnectionNetworkConfigJson);
		
		BinaryClassifierTrainingResults binaryClassifierTrainingResults = new BinaryClassifierTrainingResults(modelUuid);
		for (int iteration = 0; iteration < trainingSettings.getIterations(); iteration++) {
			trainingSet.shuffle();
			
			for (Batch batch : trainer.iterateDataset(trainingSet)) {
				EasyTrain.trainBatch(trainer, batch);
				trainer.step();
				batch.close();
			}
			
			EasyTrain.evaluateDataset(trainer, testingSet);
			
			// reset training and validation evaluators at end of epoch
			trainer.notifyListeners(listener -> listener.onEpoch(trainer));
			
			float testingF1 = metrics.getMetric("validate_epoch_BCF1").getLast().getValue().floatValue();
			float testingPrecision = metrics.getMetric("validate_epoch_BCPrecision").getLast().getValue().floatValue();
			float testingRecall = metrics.getMetric("validate_epoch_BCRecall").getLast().getValue().floatValue();
			
			String suffix = String.format("%.3f", testingF1) + "_" + iteration;
			
			BinaryClassifierEpochResult epochResult = new BinaryClassifierEpochResult(iteration, testingF1, testingPrecision, testingRecall, suffix);
			binaryClassifierTrainingResults.addEpochResult(epochResult);
			
			handleEpochResults(epochResult);
			saveModel(trainingNetwork, modelPath, epochResult);
			
			if (earlyStopTraining(binaryClassifierTrainingResults, epochResult)) {
				break;
			}
		}
		
		return binaryClassifierTrainingResults;
	}
	
	protected boolean earlyStopTraining(BinaryClassifierTrainingResults binaryClassifierTrainingResults, BinaryClassifierEpochResult epochResult) {
		// if the latest epoch is not the best F1, stop
		return binaryClassifierTrainingResults.getBestF1Epoch().epoch() != epochResult.epoch();
	}
	
	protected void saveModel(Block trainingNetwork, Path modelPath, BinaryClassifierEpochResult epochResults) throws IOException {
		try (DataOutputStream os = new DataOutputStream(new FileOutputStream(modelPath.resolve(getModelName() + "_" + epochResults.modelSuffix()).toFile()))) {
			trainingNetwork.saveParameters(os);
		}
	}
	
	protected void handleEpochResults(BinaryClassifierEpochResult epochResults) {
		LOG.info("Validation Stats Epoch {}: Testing F1: {} Testing Precision: {} Testing Recall {}", epochResults.epoch(), epochResults.f1(),
						epochResults.precision(), epochResults.recall());
	}
}
