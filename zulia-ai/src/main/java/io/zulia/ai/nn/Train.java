package io.zulia.ai.nn;

import ai.djl.MalformedModelException;
import ai.djl.inference.Predictor;
import ai.djl.translate.TranslateException;
import com.google.gson.Gson;
import io.zulia.ai.dataset.json.DenseFeatureAndCategoryDataset;
import io.zulia.ai.features.generator.ClassifierFeatureVector;
import io.zulia.ai.features.generator.ClassifierFeatureVector.BatchedClassifierFeatureVector;
import io.zulia.ai.features.scaler.FeatureScaler;
import io.zulia.ai.features.scaler.PercentileClippingFeatureScaler;
import io.zulia.ai.features.scaler.PercentileClippingFeatureScaler.NormalizeRange;
import io.zulia.ai.features.stat.FeatureStat;
import io.zulia.ai.nn.config.FullyConnectedConfiguration;
import io.zulia.ai.nn.model.BinaryClassifierModel;
import io.zulia.ai.nn.model.BinaryClassifierTrainer;
import io.zulia.ai.nn.model.BinaryClassifierTrainingResults;
import io.zulia.ai.nn.test.BinaryClassifierStats;
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
		
		boolean train = false;
		Function<FeatureStat[], FeatureScaler> featureScalerGenerator = featureStats -> new PercentileClippingFeatureScaler(featureStats,
						NormalizeRange.P10_TO_P90, 2.0);
		
		String uuid;
		String modelSuffix;
		if (train) {
			
			BinaryClassifierTrainingResults trainingResults = train(modelBaseDir, modelName, featureScalerGenerator, trainFeatures, testingFeatures);
			
			uuid = trainingResults.getUuid();
			modelSuffix = trainingResults.getBestF1Epoch().modelSuffix();
			
		}
		else {
			uuid = "d21f106d-6c95-4e7f-9f0a-08ddbbf5025a";
			modelSuffix = "0.984_2";
		}
		test(modelBaseDir, uuid, modelSuffix, modelName, featureScalerGenerator, testingFeatures);
	}
	
	private static void test(String modelBaseDir, String uuid, String modelSuffix, String modelName,
					Function<FeatureStat[], FeatureScaler> featureScalerGenerator, String testingFeatures)
					throws IOException, TranslateException, MalformedModelException {
		try (BinaryClassifierModel binaryClassifierModel = new BinaryClassifierModel(modelBaseDir, uuid, modelName, modelSuffix, featureScalerGenerator)) {
			
			int testingBatchSize = 1000;
			DenseFeatureAndCategoryDataset testingSet = new DenseFeatureAndCategoryDataset.Builder().setSampling(testingBatchSize, false)
							.setFilename(testingFeatures).build();
			
			LOG.info("Testing");
			BinaryClassifierStats binaryClassifierStats = binaryClassifierModel.evaluate(testingSet, 0.5f, testingBatchSize);
			LOG.info("Testing Stats on Loaded Model: {}", binaryClassifierStats);
			
			List<ClassifierFeatureVector> testingSample = loadTestingFeaturesSample(testingFeatures, 100);
			
			try (Predictor<float[], Float> predictor = binaryClassifierModel.getPredictor()) {
				
				//single example
				ClassifierFeatureVector test1 = testingSample.getFirst();
				Float predict = predictor.predict(test1.getFeatures());
				LOG.info("Predicted: {} with real category {}", predict, test1.getCategory());
				
				//batch example
				LOG.info("Batch predicting");
				
				BatchedClassifierFeatureVector batchedClassifierFeatureVector = ClassifierFeatureVector.asBatch(testingSample);
				List<Float> outputs = predictor.batchPredict(batchedClassifierFeatureVector.featureList());
				for (int batchIndex = 0; batchIndex < outputs.size(); batchIndex++) {
					LOG.info("{} ->  {}", String.format("%.3f", outputs.get(batchIndex)), batchedClassifierFeatureVector.getCategory(batchIndex));
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
		
		BinaryClassifierTrainer binaryClassifierTrainer = new BinaryClassifierTrainer(modelBaseDir, modelName, fullyConnectedConfiguration, trainingSettings,
						featureScalerGenerator);
		return binaryClassifierTrainer.train(trainingSet, testingSet);
		
	}
	
}
