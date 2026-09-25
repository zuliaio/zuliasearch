package io.zulia.ai.features;

import io.zulia.ai.features.scaler.MinMaxFeatureScaler;
import io.zulia.ai.features.scaler.PercentileClippingFeatureScaler;
import io.zulia.ai.features.stat.FeatureStat;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FeatureScalerTest {

	private static FeatureStat constant(double v) {
		FeatureStat stat = new FeatureStat();
		stat.setMin(v);
		stat.setMax(v);
		stat.setAvg(v);
		stat.setP05(v);
		stat.setP10(v);
		stat.setP25(v);
		stat.setP75(v);
		stat.setP90(v);
		stat.setP95(v);
		return stat;
	}

	@Test
	void constantFeatureScalesToZeroNotInfinity() {
		FeatureStat[] stats = { constant(3.0) };
		float[] minMax = new MinMaxFeatureScaler(stats).scaleFeatures(new float[] { 7.0f });
		assertEquals(0f, minMax[0]);
		assertTrue(Float.isFinite(minMax[0]));

		float[] clipped = new PercentileClippingFeatureScaler(stats, PercentileClippingFeatureScaler.NormalizeRange.P25_TO_P75, null)
				.scaleFeatures(new float[] { 7.0f });
		assertEquals(0f, clipped[0]);
	}
}
