package io.zulia.ai.nn.model.generics;

import ai.djl.MalformedModelException;
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
import io.zulia.ai.dataset.json.DenseFeatureAndCategoryDataset;
import io.zulia.ai.features.scaler.FeatureScaler;
import io.zulia.ai.features.stat.FeatureStat;
import io.zulia.ai.nn.config.FullyConnectedConfiguration;
import io.zulia.ai.nn.model.util.ModelSerializer;
import io.zulia.ai.nn.training.config.TrainingConfigurationFactory;
import io.zulia.ai.nn.training.config.TrainingSettings;
import io.zulia.data.output.FileDataOutputStream;
import io.zulia.data.target.spreadsheet.SpreadsheetTarget;
import io.zulia.data.target.spreadsheet.csv.CSVTarget;
import io.zulia.data.target.spreadsheet.csv.CSVTargetConfig;
import io.zulia.data.target.spreadsheet.delimited.formatter.NumberCSVWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

public abstract class ClassifierTrainer {
	public static final String FEATURE_STATS_JSON = "feature_stats.json";
	public static final String FULL_NETWORK_CONFIG_JSON = "full_network_config.json";
	public static final String TRAINING_SETTINGS_JSON = "training_settings.json";
	public static final String FEATURE_SCALER = "feature_scaler.json";
	private static final Logger LOG = LoggerFactory.getLogger(ClassifierTrainer.class);
	private static final int MIN_EPOCHS = 5;

	protected final String modelBaseDir;
	protected final String modelName;
	protected final FullyConnectedConfiguration fullyConnectedConfiguration;

	protected final TrainingSettings trainingSettings;
	protected final Function<FeatureStat[], FeatureScaler> featureScalerGenerator;

	public ClassifierTrainer(String modelBaseDir, String modelName, FullyConnectedConfiguration fullyConnectedConfiguration, TrainingSettings trainingSettings,
			Function<FeatureStat[], FeatureScaler> featureScalerGenerator) {
		this.modelBaseDir = modelBaseDir;
		this.modelName = modelName;
		this.fullyConnectedConfiguration = fullyConnectedConfiguration;
		this.trainingSettings = trainingSettings;
		this.featureScalerGenerator = featureScalerGenerator;
	}

	public ClassifierTrainer(String modelName, FullyConnectedConfiguration fullyConnectedConfiguration, TrainingSettings trainingSettings,
			Function<FeatureStat[], FeatureScaler> featureScalerGenerator) {
		this(null, modelName, fullyConnectedConfiguration, trainingSettings, featureScalerGenerator);
	}

	private static void trainEpoch(DenseFeatureAndCategoryDataset trainingSet, DenseFeatureAndCategoryDataset testingSet, Trainer trainer)
			throws IOException, TranslateException {
		trainingSet.shuffle();
		for (Batch batch : trainer.iterateDataset(trainingSet)) {
			EasyTrain.trainBatch(trainer, batch);
			trainer.step();
			batch.close();
		}

		EasyTrain.evaluateDataset(trainer, testingSet);

		// reset training and validation evaluators at end of epoch
		trainer.notifyListeners(listener -> listener.onEpoch(trainer));
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

	public ClassifierTrainingResults train(DenseFeatureAndCategoryDataset trainingSet, DenseFeatureAndCategoryDataset testingSet)
			throws IOException, TranslateException {

		long tic = System.currentTimeMillis();

		FeatureScaler featureScaler = featureScalerGenerator.apply(trainingSet.getFeatureStats());
		trainingSet.setFeatureScaler(featureScaler);
		testingSet.setFeatureScaler(featureScaler);

		ClassifierTrainingResults results;
		if (getTrainingSettings().getTrainInMemory()) {
			results = trainInMemory(trainingSet, testingSet, featureScaler);
		}
		else {
			results = trainWithIO(trainingSet, testingSet, featureScaler);
		}

		LOG.info("Training complete in <{}>s", (System.currentTimeMillis() - tic) / 1000.0);

		// final logging
		var bestResult = results.getBestEpoch();
		LOG.info("Best Epoch: <{}>", bestResult.epoch());
		handleEpochResults(bestResult);

		return results;
	}

	/**
	 * Do the full training excercise in memory so that something else could handle IO OR the network can be used and discarded
	 *
	 * @param trainingSet   dataset for training
	 * @param testingSet    dataset for epoch evaluation
	 * @param featureScaler scaler for features
	 * @return results of training WITH a model and scaler inside
	 */
	private ClassifierTrainingResults trainInMemory(DenseFeatureAndCategoryDataset trainingSet, DenseFeatureAndCategoryDataset testingSet,
			FeatureScaler featureScaler) throws IOException, TranslateException {
		String modelUuid = UUID.randomUUID().toString();
		ClassifierTrainingResults classifierTrainingResults = new ClassifierTrainingResults(modelUuid);

		byte[] bestModelBuffer = null;
		try (Model model = Model.newInstance(modelName)) {
			Block trainingNetwork = fullyConnectedConfiguration.getTrainingNetwork(trainingSettings);
			model.setBlock(trainingNetwork);

			DefaultTrainingConfig trainingConfig = TrainingConfigurationFactory.getTrainingConfig(trainingSettings);
			try (Trainer trainer = model.newTrainer(trainingConfig)) {
				trainer.initialize(new Shape(trainingSettings.getBatchSize(), fullyConnectedConfiguration.getNumberOfInputs()));

				Metrics metrics = new Metrics();
				trainer.setMetrics(metrics);

				// Logger for results to see live training output
				for (int iteration = 0; iteration < trainingSettings.getIterations(); iteration++) {
					trainEpoch(trainingSet, testingSet, trainer);

					// This is handled down in the weeds of the individual trainer
					ClassifierEpochResult epochResult = logResults(metrics, iteration, null, null);

					// Store results into list
					classifierTrainingResults.addEpochResult(epochResult);

					// Write and look for breakpoint
					handleEpochResults(epochResult);

					// If we just made the best epoch, then we should update the saved data
					if (classifierTrainingResults.getBestEpoch().epoch() == epochResult.epoch()) {
						bestModelBuffer = ModelSerializer.serializeModel(model, fullyConnectedConfiguration, featureScaler);
					}
					else if (iteration >= MIN_EPOCHS - 1) {
						// If the epoch just received is not the best epoch, but you've run through a few, then break out
						break;
					}
				}
			}
		}

		// pull out the model
		// NOT autocloseable because someone else needs to handle that
		try {
			// Bring up the best serialized model into a new copy
			ModelSerializer.FullModel fullModel = ModelSerializer.deserializeFullModelFromBuffer(bestModelBuffer);

			// Store out results
			classifierTrainingResults.setResultModel(fullModel.model());
			classifierTrainingResults.setResultFeatureScaler(featureScaler);
		}
		catch (MalformedModelException e) {
			LOG.error("Could not deserialize parameters from model.", e);
			throw new RuntimeException(e);
		}

		// Send output to caller
		return classifierTrainingResults;
	}

	/**
	 * More "classic" training style that write out the results of training to file system for reuse/deployment and retrieval
	 *
	 * @param trainingSet   train on these
	 * @param testingSet    validate epoch scores on these
	 * @param featureScaler scaler to log during epoch writes
	 * @return results of training (no model included)
	 */
	private ClassifierTrainingResults trainWithIO(DenseFeatureAndCategoryDataset trainingSet, DenseFeatureAndCategoryDataset testingSet,
			FeatureScaler featureScaler) throws IOException, TranslateException {
		String modelUuid = UUID.randomUUID().toString();
		Path modelPath = Path.of(modelBaseDir, modelUuid);
		Files.createDirectory(modelPath);
		LOG.info("Model directory: {}", modelPath);
		ClassifierTrainingResults classifierTrainingResults = new ClassifierTrainingResults(modelUuid);

		Gson gson = new Gson();

		try (Model model = Model.newInstance(modelName)) {
			Block trainingNetwork = fullyConnectedConfiguration.getTrainingNetwork(trainingSettings);
			model.setBlock(trainingNetwork);

			DefaultTrainingConfig trainingConfig = TrainingConfigurationFactory.getTrainingConfig(trainingSettings);

			try (Trainer trainer = model.newTrainer(trainingConfig)) {
				trainer.initialize(new Shape(trainingSettings.getBatchSize(), fullyConnectedConfiguration.getNumberOfInputs()));

				Metrics metrics = new Metrics();
				trainer.setMetrics(metrics);

				// Write metafiles for users to retrieve settings
				Files.writeString(modelPath.resolve(model.getName() + "_" + FEATURE_STATS_JSON), gson.toJson(trainingSet.getFeatureStats()));
				Files.writeString(modelPath.resolve(model.getName() + "_" + FULL_NETWORK_CONFIG_JSON), gson.toJson(fullyConnectedConfiguration));
				Files.writeString(modelPath.resolve(model.getName() + "_" + TRAINING_SETTINGS_JSON), gson.toJson(trainingSettings));
				Files.writeString(modelPath.resolve(model.getName() + "_" + FEATURE_SCALER), gson.toJson(featureScaler));

				String featureScalerDesc = featureScaler.toString();

				// Logger for results to see live training output
				FileDataOutputStream fileDataOutputStream = FileDataOutputStream.from(modelPath.resolve("results.csv"), true);
				CSVTargetConfig csvTargetConfig = CSVTargetConfig.from(fileDataOutputStream).withHeaders(getHeaders())
						.withNumberTypeHandler(new NumberCSVWriter<>().withDecimalPlaces(4));
				try (SpreadsheetTarget<?, ?> spreadsheetTarget = CSVTarget.withConfig(csvTargetConfig)) {

					for (int iteration = 0; iteration < trainingSettings.getIterations(); iteration++) {
						trainEpoch(trainingSet, testingSet, trainer);

						// This is handled down in the weeds of the individual trainer
						ClassifierEpochResult epochResult = logResults(metrics, iteration, spreadsheetTarget, featureScalerDesc);

						// Store results into list
						classifierTrainingResults.addEpochResult(epochResult);

						// Write and look for breakpoint
						handleEpochResults(epochResult);
						saveModel(trainingNetwork, modelPath, epochResult);

						if (earlyStopTraining(classifierTrainingResults, epochResult)) {
							break;
						}
					}

				}

			}

			// Send output to caller
			return classifierTrainingResults;
		}
	}

	protected abstract List<String> getHeaders();

	protected abstract ClassifierEpochResult logResults(Metrics metrics, int iteration, SpreadsheetTarget<?, ?> spreadsheetTarget, String featureScalerDesc);

	protected boolean earlyStopTraining(ClassifierTrainingResults classifierTrainingResults, ClassifierEpochResult epochResult) {
		if (epochResult.epoch() <= MIN_EPOCHS - 1) {
			return false; // Do at least a few of the requested training to see what happens
		}

		// if the latest epoch is not the best F1, stop
		return classifierTrainingResults.getBestEpoch().epoch() != epochResult.epoch();
	}

	protected void saveModel(Block trainingNetwork, Path modelPath, ClassifierEpochResult epochResults) throws IOException {
		try (DataOutputStream os = new DataOutputStream(new FileOutputStream(modelPath.resolve(getModelName() + "_" + epochResults.modelSuffix()).toFile()))) {
			trainingNetwork.saveParameters(os);
		}
	}

	protected void handleEpochResults(ClassifierEpochResult epochResults) {
		if (epochResults.f1() != null) {
			LOG.info("Epoch {}: Testing F1: {} Testing Precision: {} Testing Recall {}", epochResults.epoch(), epochResults.f1(), epochResults.precision(),
					epochResults.recall());
		}
		else {
			LOG.info("Epoch {}: Testing Accuracy: {}", epochResults.epoch(), epochResults.accuracy());
		}
	}
}
