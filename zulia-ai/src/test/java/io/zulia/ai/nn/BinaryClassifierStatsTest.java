package io.zulia.ai.nn;

import io.zulia.ai.nn.model.generics.ClassifierEpochResult;
import io.zulia.ai.nn.model.generics.ClassifierTrainingResults;
import io.zulia.ai.nn.test.BinaryClassifierStats;
import io.zulia.ai.nn.training.config.DefaultBinarySettings;
import io.zulia.ai.nn.test.BinaryClassifierAccuracy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BinaryClassifierStatsTest {

	@Test
	void statsWithEmptyDenominatorsAreZeroNotNaN() {
		BinaryClassifierStats noPositives = new BinaryClassifierStats(0, 0, 10, 5);
		assertEquals(0f, noPositives.getPrecision());
		assertEquals(0f, noPositives.getRecall());
		assertEquals(0f, noPositives.getF1());
		assertEquals(10f / 15f, noPositives.getAccuracy(), 1e-6);

		BinaryClassifierStats empty = new BinaryClassifierStats();
		assertEquals(0f, empty.getAccuracy());
		assertEquals(0f, empty.getF1());
	}

	@Test
	void accuracyComesFromTheConfusionCounts() {
		BinaryClassifierStats stats = new BinaryClassifierStats(8, 2, 6, 4);
		assertEquals(14f / 20f, stats.getAccuracy(), 1e-6);
		assertEquals(0.8f, stats.getPrecision(), 1e-6);
		assertEquals(8f / 12f, stats.getRecall(), 1e-6);
	}

	@Test
	void binarySettingsProvideAnAccuracyEvaluator() {
		assertTrue(new DefaultBinarySettings(0.5f).getEvaluators().stream().anyMatch(e -> e instanceof BinaryClassifierAccuracy),
				"binary training reads validate_epoch_" + BinaryClassifierAccuracy.BC_ACCURACY);
	}

	@Test
	void nanEpochsNeverWinBestEpochSelection() {
		ClassifierTrainingResults results = new ClassifierTrainingResults("t");
		results.addEpochResult(new ClassifierEpochResult(0, Float.NaN, Float.NaN, Float.NaN, Float.NaN, "0"));
		results.addEpochResult(new ClassifierEpochResult(1, 0.6f, 0.5f, 0.5f, 0.5f, "1"));
		results.addEpochResult(new ClassifierEpochResult(2, 0.7f, 0.4f, 0.4f, 0.4f, "2"));
		assertEquals(1, results.getBestF1Epoch().epoch());
		assertEquals(2, results.getBestAccuracyEpoch().epoch());

		ClassifierTrainingResults onlyNaN = new ClassifierTrainingResults("n");
		onlyNaN.addEpochResult(new ClassifierEpochResult(0, Float.NaN, Float.NaN, null, null, "0"));
		assertEquals(null, onlyNaN.getBestEpoch());
	}
}
