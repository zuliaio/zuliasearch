package io.zulia.ai.nn;

import ai.djl.MalformedModelException;
import ai.djl.inference.Predictor;
import ai.djl.translate.TranslateException;
import com.google.gson.Gson;
import io.zulia.ai.dataset.json.DenseFeatureAndCategoryDataset;
import io.zulia.ai.features.generator.ClassifierFeatureVector;
import io.zulia.ai.features.scaler.FeatureScaler;
import io.zulia.ai.features.scaler.PercentileClippingFeatureScaler;
import io.zulia.ai.features.scaler.PercentileClippingFeatureScaler.NormalizeRange;
import io.zulia.ai.features.stat.FeatureStat;
import io.zulia.ai.nn.config.FullyConnectedConfiguration;
import io.zulia.ai.nn.model.BinaryClassifierModel;
import io.zulia.ai.nn.model.BinaryClassifierTrainer;
import io.zulia.ai.nn.model.BinaryClassifierTrainingResults;
import io.zulia.ai.nn.training.config.DefaultBinarySettings;
import io.zulia.ai.nn.training.config.TrainingSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

public class Train {
	
	private final static Logger LOG = LoggerFactory.getLogger(Train.class);
	
	public static void main(String[] args) throws IOException, TranslateException, MalformedModelException {
		
		String trainFeatures = "/data/23.6M_feature_files/orcid2024-2_training_features.csv";
		String testingFeatures = "/data/23.6M_feature_files/orcid2024-2_testing_features.csv";
		
		String modelName = "23mil-0.001lr-0.5t";
		String modelBaseDir = "/data/disambiguation/models/23mil/";
		
		Function<FeatureStat[], FeatureScaler> featureScalerGenerator = featureStats -> new PercentileClippingFeatureScaler(featureStats,
						NormalizeRange.P10_TO_P90, 2.0);
		
		BinaryClassifierTrainingResults trainingResults = train(modelBaseDir, modelName, featureScalerGenerator, trainFeatures, testingFeatures);
		
		try (BinaryClassifierModel binaryClassifierModel = new BinaryClassifierModel(modelBaseDir, trainingResults.getUuid(), modelName, trainingResults.getBestF1Epoch().modelSuffix(),
						featureScalerGenerator)) {
			List<ClassifierFeatureVector> featureVectors = loadTestingFeaturesSample(testingFeatures, 100);
			
			try (Predictor<float[], Float> predictor = binaryClassifierModel.getPredictor()) {
				
				//single example
				ClassifierFeatureVector test1 = featureVectors.getFirst();
				Float predict = predictor.predict(test1.getFeatures());
				LOG.info("Predicted: {} with real category {}", predict, test1.getCategory());
				
				//batch example
				List<float[]> featureList = new ArrayList<>();
				List<Integer> categories = new ArrayList<>();
				for (ClassifierFeatureVector featureVector : featureVectors) {
					featureList.add(featureVector.getFeatures());
					categories.add(featureVector.getCategory());
				}
				
				LOG.info("Batch predicting");
				List<Float> outputs = predictor.batchPredict(featureList);
				for (int i = 0; i < outputs.size(); i++) {
					LOG.info("{} ->  {}", String.format("%.3f", outputs.get(i)), categories.get(i));
				}
			}
		}
	}
	
	private static List<ClassifierFeatureVector> loadTestingFeaturesSample(String testingFeatures, int firstN) throws IOException {
		List<ClassifierFeatureVector> featureVectors = new ArrayList<>();
		
		Gson gson = new Gson();
		AtomicInteger count = new AtomicInteger();
		try (Stream<String> lines = Files.lines(Paths.get(testingFeatures))) {
			lines.forEach(s -> {
				if (count.getAndIncrement() < firstN) {
					ClassifierFeatureVector v = gson.fromJson(s, ClassifierFeatureVector.class);
					featureVectors.add(v);
				}
			});
		}
		return featureVectors;
	}
	
	private static BinaryClassifierTrainingResults train(String modelBaseDir, String modelName, Function<FeatureStat[], FeatureScaler> featureScalerGenerator,
					String trainFeatures, String testingFeatures) throws IOException, TranslateException {
		
		TrainingSettings trainingSettings = new DefaultBinarySettings(0.5f).setFixedLearningRate(0.001f).setIterations(8).setBatchSize(1000);
		
		DenseFeatureAndCategoryDataset trainingSet = new DenseFeatureAndCategoryDataset.Builder().setSampling(trainingSettings.getBatchSize(), false)
						.setFilename(trainFeatures).build();
		
		DenseFeatureAndCategoryDataset testingSet = new DenseFeatureAndCategoryDataset.Builder().setSampling(trainingSettings.getBatchSize(), false)
						.setFilename(testingFeatures).build();
		
		FullyConnectedConfiguration fullyConnectedConfiguration = new FullyConnectedConfiguration(List.of(trainingSet.getNumberOfFeatures(), 40),
						trainingSet.getNumberOfFeatures(), 1).setSigmoidOutput(true);
		
		BinaryClassifierTrainer binaryClassifierTrainer = new BinaryClassifierTrainer(modelBaseDir, modelName, fullyConnectedConfiguration, trainingSettings, featureScalerGenerator);
		return binaryClassifierTrainer.train(trainingSet, testingSet);
		
	}
	
}
