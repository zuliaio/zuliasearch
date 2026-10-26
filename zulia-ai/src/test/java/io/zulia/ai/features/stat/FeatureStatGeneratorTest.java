package io.zulia.ai.features.stat;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FeatureStatGeneratorTest {

	@Test
	public void nanFeatureIsLeftOutOfTheStats() {
		FeatureStatGenerator generator = new FeatureStatGenerator(2);
		generator.addExample(new double[] { 1.0, 10.0 });
		generator.addExample(new double[] { Double.NaN, 20.0 });
		generator.addExample(new double[] { 3.0, Double.POSITIVE_INFINITY });
		generator.addExample(new float[] { Float.NaN, 30.0f });

		FeatureStat[] stats = generator.computeFeatureStats();

		Assertions.assertEquals(1.0, stats[0].getMin(), 1e-9);
		Assertions.assertEquals(3.0, stats[0].getMax(), 1e-9);
		Assertions.assertEquals(2.0, stats[0].getAvg(), 1e-9);
		Assertions.assertEquals(10.0, stats[1].getMin(), 1e-9);
		Assertions.assertEquals(30.0, stats[1].getMax(), 1e-9);
		Assertions.assertEquals(20.0, stats[1].getAvg(), 1e-9);
		Assertions.assertEquals(20.0, stats[1].getP50(), 20.0 * 1e-3);
	}

	@Test
	public void featureWithNoFiniteValuesReportsNan() {
		FeatureStatGenerator generator = new FeatureStatGenerator(2);
		generator.addExample(new double[] { Double.NaN, 1.0 });
		generator.addExample(new double[] { Double.NEGATIVE_INFINITY, 2.0 });

		FeatureStat[] stats = generator.computeFeatureStats();

		Assertions.assertTrue(Double.isNaN(stats[0].getMin()));
		Assertions.assertTrue(Double.isNaN(stats[0].getMax()));
		Assertions.assertTrue(Double.isNaN(stats[0].getAvg()));
		Assertions.assertTrue(Double.isNaN(stats[0].getP05()));
		Assertions.assertTrue(Double.isNaN(stats[0].getP95()));
		Assertions.assertEquals(1.0, stats[1].getMin(), 1e-9);
		Assertions.assertEquals(2.0, stats[1].getMax(), 1e-9);
	}

	@Test
	public void wrongFeatureCountIsRejected() {
		FeatureStatGenerator generator = new FeatureStatGenerator(2);
		Assertions.assertThrows(IllegalArgumentException.class, () -> generator.addExample(new double[] { 1.0 }));
	}
}
