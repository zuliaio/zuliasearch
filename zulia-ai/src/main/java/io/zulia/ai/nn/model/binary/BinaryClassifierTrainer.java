package io.zulia.ai.nn.model.binary;

import ai.djl.metric.Metrics;
import io.zulia.ai.features.scaler.FeatureScaler;
import io.zulia.ai.features.stat.FeatureStat;
import io.zulia.ai.nn.config.FullyConnectedConfiguration;
import io.zulia.ai.nn.model.generics.ClassifierEpochResult;
import io.zulia.ai.nn.model.generics.ClassifierTrainer;
import io.zulia.ai.nn.test.BinaryClassifierAccuracy;
import io.zulia.ai.nn.test.BinaryClassifierF1;
import io.zulia.ai.nn.test.BinaryClassifierPrecision;
import io.zulia.ai.nn.test.BinaryClassifierRecall;
import io.zulia.ai.nn.training.config.TrainingSettings;
import io.zulia.data.target.spreadsheet.SpreadsheetTarget;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Function;

public class BinaryClassifierTrainer extends ClassifierTrainer {
	private final static Logger LOG = LoggerFactory.getLogger(BinaryClassifierTrainer.class);

	public BinaryClassifierTrainer(String modelBaseDir, String modelName, FullyConnectedConfiguration fullyConnectedConfiguration,
			TrainingSettings trainingSettings, Function<FeatureStat[], FeatureScaler> featureScalerGenerator) {
		super(modelBaseDir, modelName, fullyConnectedConfiguration, trainingSettings, featureScalerGenerator);
	}

	@Override
	protected ClassifierEpochResult logResults(Metrics metrics, int iteration, SpreadsheetTarget<?, ?> spreadsheetTarget, String featureScalerDesc) {
		// the binary evaluators from DefaultBinarySettings, any of which custom settings may leave out
		Float testingAccuracy = latestMetric(metrics, BinaryClassifierAccuracy.BC_ACCURACY);
		Float testingF1 = latestMetric(metrics, BinaryClassifierF1.BCF1);
		Float testingPrecision = latestMetric(metrics, BinaryClassifierPrecision.BC_PRECISION);
		Float testingRecall = latestMetric(metrics, BinaryClassifierRecall.BC_RECALL);

		String suffix = String.format("%.3f", testingF1) + "_" + iteration;

		ClassifierEpochResult epochResult = new ClassifierEpochResult(iteration, testingAccuracy, testingF1, testingPrecision, testingRecall, suffix);

		if (spreadsheetTarget != null) {
			spreadsheetTarget.writeRow(modelName, epochResult.epoch(), epochResult.f1(), epochResult.precision(), epochResult.recall(),
					epochResult.modelSuffix(), featureScalerDesc);
		}
		return epochResult;
	}

	private static Float latestMetric(Metrics metrics, String evaluatorName) {
		String metricName = "validate_epoch_" + evaluatorName;
		if (!metrics.hasMetric(metricName)) {
			return null;
		}
		return metrics.getMetric(metricName).getLast().getValue().floatValue();
	}

	@Override
	protected List<String> getHeaders() {
		// Unique headers for unique metrics
		return List.of("Model Name", "Epoch", "F1", "Precision", "Recall", "Model Suffix", "Feature Scaler");
	}
}
