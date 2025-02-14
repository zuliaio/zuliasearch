package io.zulia.ai.nn;

import ai.djl.Model;
import ai.djl.metric.Metrics;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Block;
import ai.djl.training.DefaultTrainingConfig;
import ai.djl.training.EasyTrain;
import ai.djl.training.Trainer;
import ai.djl.training.TrainingConfig;
import ai.djl.training.dataset.Batch;
import ai.djl.training.listener.SaveModelTrainingListener;
import ai.djl.translate.TranslateException;
import io.zulia.ai.dataset.TrainingVectorDataset;
import io.zulia.ai.features.scaler.FeatureScaler;
import io.zulia.ai.features.scaler.PercentileClippingFeatureScaler;
import io.zulia.ai.features.scaler.PercentileClippingFeatureScaler.NormalizeRange;
import io.zulia.ai.nn.config.FullyConnectedConfiguration;
import io.zulia.ai.nn.training.config.DefaultBinarySettings;
import io.zulia.ai.nn.training.config.TrainingConfigurationFactory;
import io.zulia.ai.nn.training.config.TrainingSettings;

import java.io.IOException;
import java.util.List;

public class Train {
	
	public static void main(String[] args) throws IOException, TranslateException {
		int iterations = 8;
		int batchSize = 1000;
		
		
		/*
		TrainingVectorDataset trainingSet = new TrainingVectorDataset.Builder().setSampling(batchSize, true)
						.setFilename("/data/400k_training/orcid2024-2_training_features.csv").build();
		TrainingVectorDataset testingSet = new TrainingVectorDataset.Builder().setSampling(batchSize, true)
						.setFilename("/data/400k_training/orcid2024-2_testing_features.csv").build();
		 */
		
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
		
		try (Model model = Model.newInstance("23mil-0.001lr-0.5t")) {
			
			Block trainingNetwork = fullyConnectedConfiguration.getTrainingNetwork(trainingSettings);
			model.setBlock(trainingNetwork);
			
			DefaultTrainingConfig trainingConfig = TrainingConfigurationFactory.getTrainingConfig(trainingSettings);
			trainingConfig.addTrainingListeners(new SaveModelTrainingListener("/data/disambiguation/models/23mil/"));
			
			Trainer trainer = model.newTrainer(trainingConfig);
			trainer.initialize(new Shape(batchSize, fullyConnectedConfiguration.getNumberOfInputs()));
			
			
			
			Metrics metrics = new Metrics();
			trainer.setMetrics(metrics);
			
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
				
				System.out.println("F1: " + metrics.getMetric("validate_epoch_BCF1").getLast().getValue());
				System.out.println("Precision: " + metrics.getMetric("validate_epoch_BCPrecision").getLast().getValue());
				System.out.println("Recall: " + metrics.getMetric("validate_epoch_BCRecall").getLast().getValue());
				
			}
		}
		
	}
	
}
