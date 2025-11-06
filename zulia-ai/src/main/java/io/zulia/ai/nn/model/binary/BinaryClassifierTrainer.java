package io.zulia.ai.nn.model.binary;

import ai.djl.metric.Metrics;
import io.zulia.ai.features.scaler.FeatureScaler;
import io.zulia.ai.features.stat.FeatureStat;
import io.zulia.ai.nn.config.FullyConnectedConfiguration;
import io.zulia.ai.nn.model.generics.ClassifierEpochResult;
import io.zulia.ai.nn.model.generics.ClassifierTrainer;
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
		// Grabs unique training metrics specific to binary classifier
		float testingAccuracy = metrics.getMetric("validate_epoch_Accuracy").getLast().getValue().floatValue();
		float testingF1 = metrics.getMetric("validate_epoch_" + BinaryClassifierF1.BCF1).getLast().getValue().floatValue();
		float testingPrecision = metrics.getMetric("validate_epoch_" + BinaryClassifierPrecision.BC_PRECISION).getLast().getValue().floatValue();
		float testingRecall = metrics.getMetric("validate_epoch_" + BinaryClassifierRecall.BC_RECALL).getLast().getValue().floatValue();

		String suffix = String.format("%.3f", testingF1) + "_" + iteration;

		ClassifierEpochResult epochResult = new ClassifierEpochResult(iteration, testingAccuracy, testingF1, testingPrecision, testingRecall, suffix);

		spreadsheetTarget.writeRow(modelName, epochResult.epoch(), epochResult.f1(), epochResult.precision(), epochResult.recall(), epochResult.modelSuffix(),
				featureScalerDesc);
		return epochResult;
	}

	@Override
	protected List<String> getHeaders() {
		// Unique headers for unique metrics
		return List.of("Model Name", "Epoch", "F1", "Precision", "Recall", "Model Suffix", "Feature Scaler");
	}
}
