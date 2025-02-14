package io.zulia.ai.nn;

import ai.djl.MalformedModelException;
import ai.djl.Model;
import ai.djl.inference.Predictor;
import ai.djl.metric.Metrics;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Block;
import ai.djl.training.DefaultTrainingConfig;
import ai.djl.training.EasyTrain;
import ai.djl.training.Trainer;
import ai.djl.training.dataset.Batch;
import ai.djl.translate.TranslateException;
import ai.djl.translate.Translator;
import ai.djl.translate.TranslatorContext;
import com.google.gson.Gson;
import io.zulia.ai.dataset.TrainingVectorDataset;
import io.zulia.ai.features.generator.ClassifierFeatureVector;
import io.zulia.ai.features.scaler.FeatureScaler;
import io.zulia.ai.features.scaler.PercentileClippingFeatureScaler;
import io.zulia.ai.features.scaler.PercentileClippingFeatureScaler.NormalizeRange;
import io.zulia.ai.features.stat.FeatureStat;
import io.zulia.ai.nn.config.FullyConnectedConfiguration;
import io.zulia.ai.nn.training.config.DefaultBinarySettings;
import io.zulia.ai.nn.training.config.TrainingConfigurationFactory;
import io.zulia.ai.nn.training.config.TrainingSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class Train {
	
	private final static Logger LOG = LoggerFactory.getLogger(Train.class);
	
	public static void main(String[] args) throws IOException, TranslateException, MalformedModelException {
		//train();
		load();
	}
	
	private static void load() throws IOException, MalformedModelException, TranslateException {
		
		Gson gson = new Gson();
		
		String modelDir = "6898e43e-7007-464b-94ff-99f7e30f9428";
		Path modelPath = Path.of("/data/disambiguation/models/23mil/" + modelDir);
		
		String modelName = "23mil-0.001lr-0.5t";
		String paramsName = modelName + "_" + "0.983_1";
		try (Model finalModel = Model.newInstance(modelName)) {
			Path featureStatPath = modelPath.resolve(modelName + "_" + "feature_stats.json");
			FeatureStat[] featureStats = gson.fromJson(Files.readString(featureStatPath), FeatureStat[].class);
			FeatureScaler featureScaler = new PercentileClippingFeatureScaler(featureStats, NormalizeRange.P10_TO_P90, 2.0);
			
			Path fullConfigPath = modelPath.resolve(modelName + "_" + "full_network_config.json");
			FullyConnectedConfiguration fullyConnectedConfiguration = gson.fromJson(Files.readString(fullConfigPath), FullyConnectedConfiguration.class);
			
			Block evaluationNetwork = fullyConnectedConfiguration.getEvaluationNetwork();
			evaluationNetwork.loadParameters(finalModel.getNDManager(), new DataInputStream(new FileInputStream(modelPath.resolve(paramsName).toFile())));
			finalModel.setBlock(evaluationNetwork);
			
			Predictor<float[], Float> predictor = finalModel.newPredictor(new Translator<>() {
				
				@Override
				public NDList processInput(TranslatorContext ctx, float[] input) {
					float[] scaledFeatures = featureScaler.scaleFeatures(input);
					return new NDList(ctx.getNDManager().create(scaledFeatures));
				}
				
				@Override
				public Float processOutput(TranslatorContext ctx, NDList list) {
					return list.getFirst().getFloat(0);
				}
			});
			
			List<ClassifierFeatureVector> featureVectors = new ArrayList<>();
			
			AtomicInteger count = new AtomicInteger();
			try (Stream<String> lines = Files.lines(Paths.get("/data/23.6M_feature_files/orcid2024-2_testing_features.csv"))) {
				lines.forEach(s -> {
					if (count.getAndIncrement() < 100) {
						ClassifierFeatureVector v = gson.fromJson(s, ClassifierFeatureVector.class);
						featureVectors.add(v);
					}
				});
			}
			
			//single example
			ClassifierFeatureVector test1 = featureVectors.getFirst();
			Float predict = predictor.predict(test1.getFeatures());
			System.out.println("Predicted: " + predict + " with real category " + test1.getCategory());
			
			//batch example
			List<float[]> featureList = new ArrayList<>();
			List<Integer> categories = new ArrayList<>();
			for (ClassifierFeatureVector featureVector : featureVectors) {
				featureList.add(featureVector.getFeatures());
				categories.add(featureVector.getCategory());
			}
			
			System.out.println("Batch predicting");
			List<Float> outputs = predictor.batchPredict(featureList);
			for (int i = 0; i < outputs.size(); i++) {
				System.out.println(outputs.get(i) + " ->  " + categories.get(i));
			}
		}
	}
	
	private static void train() throws IOException, TranslateException {
		int iterations = 8;
		int batchSize = 1000;
		
		Path modelPath = Path.of("/data/disambiguation/models/23mil/" + UUID.randomUUID());
		Files.createDirectory(modelPath);
		LOG.info("Model directory: {}", modelPath);
		
		TrainingVectorDataset trainingSet = new TrainingVectorDataset.Builder().setSampling(batchSize, false)
						.setFilename("/data/23.6M_feature_files/orcid2024-2_training_features.csv").build();
		
		FeatureScaler featureScaler = new PercentileClippingFeatureScaler(trainingSet.getFeatureStats(), NormalizeRange.P10_TO_P90, 2.0);
		trainingSet.setFeatureScaler(featureScaler);
		
		TrainingVectorDataset testingSet = new TrainingVectorDataset.Builder().setSampling(batchSize, false)
						.setFilename("/data/23.6M_feature_files/orcid2024-2_testing_features.csv").build();
		testingSet.setFeatureScaler(featureScaler);
		
		TrainingSettings trainingSettings = new DefaultBinarySettings(0.5f).setFixedLearningRate(0.001f);
		FullyConnectedConfiguration fullyConnectedConfiguration = new FullyConnectedConfiguration(List.of(trainingSet.getNumberOfFeatures(), 40),
						trainingSet.getNumberOfFeatures(), 1);
		
		Gson gson = new Gson();
		try (Model model = Model.newInstance("23mil-0.001lr-0.5t")) {
			
			Block trainingNetwork = fullyConnectedConfiguration.getTrainingNetwork(trainingSettings);
			model.setBlock(trainingNetwork);
			
			DefaultTrainingConfig trainingConfig = TrainingConfigurationFactory.getTrainingConfig(trainingSettings);
			
			Trainer trainer = model.newTrainer(trainingConfig);
			trainer.initialize(new Shape(batchSize, fullyConnectedConfiguration.getNumberOfInputs()));
			
			Metrics metrics = new Metrics();
			trainer.setMetrics(metrics);
			
			String featureStatJson = gson.toJson(trainingSet.getFeatureStats());
			Files.writeString(modelPath.resolve(model.getName() + "_" + "feature_stats.json"), featureStatJson);
			
			String fullConnectionNetworkConfigJson = gson.toJson(fullyConnectedConfiguration);
			Files.writeString(modelPath.resolve(model.getName() + "_" + "full_network_config.json"), fullConnectionNetworkConfigJson);
			
			for (int iteration = 0; iteration < iterations; iteration++) {
				trainingSet.shuffle();
				
				for (Batch batch : trainer.iterateDataset(trainingSet)) {
					EasyTrain.trainBatch(trainer, batch);
					trainer.step();
					batch.close();
				}
				
				EasyTrain.evaluateDataset(trainer, testingSet);
				
				// reset training and validation evaluators at end of epoch
				trainer.notifyListeners(listener -> listener.onEpoch(trainer));
				
				LOG.info("Validation Stats Epoch {}", iteration);
				Double testingF1 = metrics.getMetric("validate_epoch_BCF1").getLast().getValue();
				Double testingPrecision = metrics.getMetric("validate_epoch_BCPrecision").getLast().getValue();
				Double testingRecall = metrics.getMetric("validate_epoch_BCRecall").getLast().getValue();
				LOG.info("F1: {}", testingF1);
				LOG.info("Precision: {}", testingPrecision);
				LOG.info("Recall: {}", testingRecall);
				
				trainingNetwork.saveParameters(new DataOutputStream(
								new FileOutputStream(modelPath.resolve(model.getName() + "_" + String.format("%.3f", testingF1) + "_" + iteration).toFile())));
			}
		}
	}
	
}
