package io.zulia.ai.nn.model.multi;

import ai.djl.metric.Metrics;
import io.zulia.ai.features.scaler.FeatureScaler;
import io.zulia.ai.features.stat.FeatureStat;
import io.zulia.ai.nn.config.FullyConnectedConfiguration;
import io.zulia.ai.nn.model.generics.ClassifierEpochResult;
import io.zulia.ai.nn.model.generics.ClassifierTrainer;
import io.zulia.ai.nn.training.config.TrainingSettings;
import io.zulia.data.target.spreadsheet.SpreadsheetTarget;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Function;

public class MultiClassifierTrainer extends ClassifierTrainer {
	private final static Logger LOG = LoggerFactory.getLogger(MultiClassifierTrainer.class);

	public MultiClassifierTrainer(String modelBaseDir, String modelName, FullyConnectedConfiguration fullyConnectedConfiguration,
			TrainingSettings trainingSettings, Function<FeatureStat[], FeatureScaler> featureScalerGenerator) {
		super(modelBaseDir, modelName, fullyConnectedConfiguration, trainingSettings, featureScalerGenerator);
	}

	@Override
	protected ClassifierEpochResult logResults(Metrics metrics, int iteration, SpreadsheetTarget<?, ?> spreadsheetTarget, String featureScalerDesc) {
		// only metric so far is accuracy, others could be added easily enough
		float testingAccuracy = metrics.getMetric("validate_epoch_Accuracy").getLast().getValue().floatValue();

		String suffix = String.format("%.3f", testingAccuracy) + "_" + iteration;

		ClassifierEpochResult epochResult = new ClassifierEpochResult(iteration, testingAccuracy, null, null, null, suffix);

		spreadsheetTarget.writeRow(modelName, epochResult.epoch(), testingAccuracy, epochResult.modelSuffix(), featureScalerDesc);
		return epochResult;
	}

	@Override
	protected List<String> getHeaders() {
		// Unique headers since this table will have different active fields
		return List.of("Model Name", "Epoch", "Accuracy", "Model Suffix", "Feature Scaler");
	}

}