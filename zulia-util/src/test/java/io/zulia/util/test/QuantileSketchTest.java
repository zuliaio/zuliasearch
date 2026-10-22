package io.zulia.util.test;

import io.zulia.util.sketch.QuantileSketch;
import io.zulia.util.sketch.QuantileSketch.Mapping;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Random;

public class QuantileSketchTest {

	private static final double ACCURACY = 0.001;
	private static final double[] QUANTILES = { 0, 0.01, 0.25, 0.5, 0.75, 0.99, 1 };

	@ParameterizedTest
	@EnumSource(Mapping.class)
	void quantilesAreWithinTheRelativeAccuracy(Mapping mapping) {
		Random random = new Random(3);
		for (int trial = 0; trial < 50; trial++) {
			double[] values = randomValues(random, 1 + random.nextInt(5000));
			QuantileSketch sketch = new QuantileSketch(ACCURACY, mapping);
			for (double value : values) {
				sketch.accept(value);
			}
			double[] sorted = values.clone();
			Arrays.sort(sorted);
			double[] quantiles = sketch.quantiles(QUANTILES);
			for (int i = 0; i < QUANTILES.length; i++) {
				double exact = sorted[(int) (QUANTILES[i] * (sorted.length - 1))];
				Assertions.assertEquals(exact, quantiles[i], Math.abs(exact) * sketch.relativeAccuracy() + 1e-12,
						mapping + " trial " + trial + " quantile " + QUANTILES[i]);
				Assertions.assertEquals(quantiles[i], sketch.quantile(QUANTILES[i]), 0.0, "single quantile matches the batch");
			}
			Assertions.assertTrue(sketch.relativeAccuracy() <= ACCURACY, "guarantee is never worse than requested");
		}
	}

	@Test
	void countSumMinMaxAreExact() {
		Random random = new Random(5);
		double[] values = randomValues(random, 10000);
		QuantileSketch sketch = new QuantileSketch(ACCURACY);
		double sum = 0;
		for (double value : values) {
			sketch.accept(value);
			sum += value;
		}
		double[] sorted = values.clone();
		Arrays.sort(sorted);
		Assertions.assertEquals(values.length, sketch.count());
		Assertions.assertEquals(sum, sketch.sum(), 0.0);
		Assertions.assertEquals(sum / values.length, sketch.average(), 0.0);
		Assertions.assertEquals(sorted[0], sketch.min(), 0.0);
		Assertions.assertEquals(sorted[sorted.length - 1], sketch.max(), 0.0);
	}

	@ParameterizedTest
	@EnumSource(Mapping.class)
	void mergingShardsMatchesOneSketchOverEverything(Mapping mapping) {
		Random random = new Random(7);
		for (int trial = 0; trial < 100; trial++) {
			QuantileSketch all = new QuantileSketch(ACCURACY, mapping);
			QuantileSketch mergedObjects = new QuantileSketch(ACCURACY, mapping);
			QuantileSketch mergedBytes = new QuantileSketch(ACCURACY, mapping);
			int shards = 1 + random.nextInt(8);
			for (int shard = 0; shard < shards; shard++) {
				QuantileSketch part = new QuantileSketch(ACCURACY, mapping);
				// some shards have nothing, so empty merges and empty byte forms are covered
				double[] values = random.nextInt(4) == 0 ? new double[0] : randomValues(random, random.nextInt(500));
				for (double value : values) {
					all.accept(value);
					part.accept(value);
				}
				mergedObjects.mergeWith(part);
				mergedBytes.mergeFrom(part.toBytes());
			}
			assertSameSketch(all, mergedObjects, mapping + " trial " + trial + " mergeWith");
			assertSameSketch(all, mergedBytes, mapping + " trial " + trial + " mergeFrom");
		}
	}

	@ParameterizedTest
	@EnumSource(Mapping.class)
	void bytesRoundTrip(Mapping mapping) {
		Random random = new Random(11);
		for (int trial = 0; trial < 100; trial++) {
			QuantileSketch sketch = new QuantileSketch(ACCURACY, mapping);
			for (double value : randomValues(random, random.nextInt(2000))) {
				sketch.accept(value);
			}
			// far apart values leave long runs of empty bins
			if (random.nextBoolean()) {
				sketch.accept(1e-6);
				sketch.accept(1e9);
				sketch.accept(-1e-6);
				sketch.accept(-1e9);
			}
			QuantileSketch copy = QuantileSketch.fromBytes(sketch.toBytes());
			assertSameSketch(sketch, copy, mapping + " trial " + trial);
			Assertions.assertEquals(mapping, copy.mapping());
			Assertions.assertEquals(sketch.relativeAccuracy(), copy.relativeAccuracy(), 0.0);
		}
	}

	@Test
	void emptySketchRoundTripsAndRejectsStatistics() {
		QuantileSketch empty = QuantileSketch.fromBytes(new QuantileSketch(ACCURACY).toBytes());
		Assertions.assertTrue(empty.isEmpty());
		Assertions.assertEquals(0, empty.count());
		Assertions.assertEquals(0, empty.bins());
		Assertions.assertEquals(0.0, empty.sum(), 0.0);
		Assertions.assertThrows(NoSuchElementException.class, empty::average);
		Assertions.assertThrows(NoSuchElementException.class, empty::min);
		Assertions.assertThrows(NoSuchElementException.class, empty::max);
		Assertions.assertThrows(NoSuchElementException.class, () -> empty.quantile(0.5));

		// merging an empty sketch into a populated one must not disturb min or max
		QuantileSketch populated = new QuantileSketch(ACCURACY);
		populated.accept(3);
		populated.mergeWith(empty);
		populated.mergeFrom(empty.toBytes());
		Assertions.assertEquals(3, populated.min(), 0.0);
		Assertions.assertEquals(3, populated.max(), 0.0);
		Assertions.assertEquals(1, populated.count());
	}

	@Test
	void zeroAndTinyValuesCountAsZero() {
		QuantileSketch sketch = new QuantileSketch(ACCURACY);
		sketch.accept(0);
		sketch.accept(Double.MIN_VALUE);
		sketch.accept(-Double.MIN_VALUE);
		sketch.accept(5);
		Assertions.assertEquals(4, sketch.count());
		Assertions.assertEquals(0, sketch.quantile(0), 0.0);
		Assertions.assertEquals(0, sketch.quantile(0.5), 0.0);
		Assertions.assertEquals(5, sketch.quantile(1), 5 * ACCURACY);
		Assertions.assertEquals(-Double.MIN_VALUE, sketch.min(), 0.0);
	}

	@Test
	void differentShapesAreRejected() {
		QuantileSketch fine = new QuantileSketch(0.001);
		fine.accept(1);
		QuantileSketch coarse = new QuantileSketch(0.01);
		coarse.accept(1);
		QuantileSketch linear = new QuantileSketch(0.001, Mapping.LINEAR);
		linear.accept(1);

		IllegalArgumentException accuracy = Assertions.assertThrows(IllegalArgumentException.class, () -> fine.mergeWith(coarse));
		Assertions.assertTrue(accuracy.getMessage().contains("0.001") && accuracy.getMessage().contains("0.01"), accuracy.getMessage());
		IllegalArgumentException mapping = Assertions.assertThrows(IllegalArgumentException.class, () -> fine.mergeFrom(linear.toBytes()));
		Assertions.assertTrue(mapping.getMessage().contains("CUBIC") && mapping.getMessage().contains("LINEAR"), mapping.getMessage());
		Assertions.assertEquals(1, fine.count(), "a rejected merge leaves the sketch untouched");
	}

	@Test
	void badInputIsRejected() {
		QuantileSketch sketch = new QuantileSketch(ACCURACY);
		Assertions.assertThrows(IllegalArgumentException.class, () -> sketch.accept(Double.NaN));
		Assertions.assertThrows(IllegalArgumentException.class, () -> sketch.accept(Double.POSITIVE_INFINITY));
		Assertions.assertThrows(IllegalArgumentException.class, () -> sketch.accept(Double.NEGATIVE_INFINITY));
		Assertions.assertThrows(IllegalArgumentException.class, () -> new QuantileSketch(0));
		Assertions.assertThrows(IllegalArgumentException.class, () -> new QuantileSketch(1));
		sketch.accept(1);
		Assertions.assertThrows(IllegalArgumentException.class, () -> sketch.quantile(1.5));
		Assertions.assertThrows(IllegalArgumentException.class, () -> sketch.quantiles(new double[] { 0.9, 0.1 }));
		Assertions.assertThrows(IllegalArgumentException.class, () -> QuantileSketch.fromBytes(new byte[] { 9 }));
	}

	@Test
	void byteFormIsCompactForSparseBins() {
		QuantileSketch sketch = new QuantileSketch(ACCURACY);
		sketch.accept(1);
		sketch.accept(1e6);
		int bins = sketch.bins();
		Assertions.assertTrue(bins > 1000, "two values six decades apart span thousands of bins, got " + bins);
		int bytes = sketch.toBytes().length;
		Assertions.assertTrue(bytes < 64, "empty bins collapse to a run, got " + bytes + " bytes for " + bins + " bins");
	}

	/** Positive, negative or mixed values over a random magnitude so bin ranges overlap or sit far apart. */
	private static double[] randomValues(Random random, int size) {
		double scale = Math.pow(10, random.nextInt(9) - 3);
		int sign = random.nextInt(3);
		double[] values = new double[size];
		for (int i = 0; i < size; i++) {
			double magnitude = Math.exp(random.nextGaussian()) * scale;
			values[i] = switch (sign) {
				case 0 -> magnitude;
				case 1 -> -magnitude;
				default -> random.nextBoolean() ? magnitude : -magnitude;
			};
		}
		return values;
	}

	private static void assertSameSketch(QuantileSketch expected, QuantileSketch actual, String message) {
		Assertions.assertEquals(expected.count(), actual.count(), message + " count");
		Assertions.assertEquals(expected.bins(), actual.bins(), message + " bins");
		Assertions.assertEquals(expected.sum(), actual.sum(), Math.abs(expected.sum()) * 1e-12, message + " sum");
		if (!expected.isEmpty()) {
			Assertions.assertEquals(expected.min(), actual.min(), 0.0, message + " min");
			Assertions.assertEquals(expected.max(), actual.max(), 0.0, message + " max");
			Assertions.assertArrayEquals(expected.quantiles(QUANTILES), actual.quantiles(QUANTILES), 0.0, message + " quantiles");
		}
	}
}
