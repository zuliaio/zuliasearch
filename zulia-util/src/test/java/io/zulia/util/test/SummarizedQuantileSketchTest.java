package io.zulia.util.test;

import io.zulia.util.sketch.QuantileSketch;
import io.zulia.util.sketch.QuantileSketch.Mapping;
import io.zulia.util.sketch.Quantiles;
import io.zulia.util.sketch.SummarizedQuantileSketch;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Random;

public class SummarizedQuantileSketchTest {

	private static final double ACCURACY = 0.001;
	private static final double[] QUANTILES = { 0, 0.01, 0.25, 0.5, 0.75, 0.99, 1 };

	@Test
	void summaryIsExactAndQuantilesMatchThePlainSketch() {
		double[] values = randomValues(new Random(17), 10000);
		SummarizedQuantileSketch summarized = new SummarizedQuantileSketch(ACCURACY);
		QuantileSketch plain = new QuantileSketch(ACCURACY);
		double sum = 0;
		for (double value : values) {
			summarized.accept(value);
			plain.accept(value);
			sum += value;
		}
		Assertions.assertEquals(values.length, summarized.count());
		Assertions.assertEquals(sum, summarized.sum(), 0.0);
		Assertions.assertEquals(sum / values.length, summarized.average(), 0.0);
		Assertions.assertEquals(Arrays.stream(values).min().orElseThrow(), summarized.min(), 0.0);
		Assertions.assertEquals(Arrays.stream(values).max().orElseThrow(), summarized.max(), 0.0);
		Assertions.assertEquals(plain.bins(), summarized.bins());
		Assertions.assertArrayEquals(plain.quantiles(QUANTILES), summarized.quantiles(QUANTILES), 0.0);
		Assertions.assertEquals(plain.quantile(0.5), summarized.quantile(0.5), 0.0);
	}

	@Test
	void bothKindsReadAlikeThroughQuantiles() {
		double[] values = randomValues(new Random(31), 3000);
		Quantiles plain = new QuantileSketch(ACCURACY, Mapping.LOG);
		Quantiles summarized = new SummarizedQuantileSketch(ACCURACY, Mapping.LOG);
		Arrays.stream(values).forEach(plain.andThen(summarized));
		Assertions.assertEquals(values.length, plain.count());
		Assertions.assertEquals(plain.count(), summarized.count());
		Assertions.assertEquals(plain.mapping(), summarized.mapping());
		Assertions.assertEquals(plain.relativeAccuracy(), summarized.relativeAccuracy(), 0.0);
		Assertions.assertArrayEquals(plain.quantiles(QUANTILES), summarized.quantiles(QUANTILES), 0.0);
		String kind = switch (summarized) {
			case QuantileSketch _ -> "plain";
			case SummarizedQuantileSketch s -> "summarized with max " + s.max();
		};
		Assertions.assertTrue(kind.startsWith("summarized"), kind);
	}

	@Test
	void summaryRidesAlongInBytesAndMerges() {
		double[] values = randomValues(new Random(23), 5000);
		SummarizedQuantileSketch sketch = new SummarizedQuantileSketch(ACCURACY, Mapping.LINEAR);
		for (double value : values) {
			sketch.accept(value);
		}
		SummarizedQuantileSketch copy = SummarizedQuantileSketch.fromBytes(sketch.toBytes());
		assertSameSketch(sketch, copy);
		Assertions.assertEquals(Mapping.LINEAR, copy.mapping());

		SummarizedQuantileSketch merged = new SummarizedQuantileSketch(ACCURACY, Mapping.LINEAR);
		merged.mergeFrom(sketch.toBytes());
		merged.mergeWith(copy);
		Assertions.assertEquals(2L * values.length, merged.count());
		Assertions.assertEquals(2 * sketch.sum(), merged.sum(), Math.abs(sketch.sum()) * 1e-15);
		Assertions.assertEquals(sketch.min(), merged.min(), 0.0);
		Assertions.assertEquals(sketch.max(), merged.max(), 0.0);
	}

	@Test
	void emptySketchHasNoAverageMinOrMax() {
		SummarizedQuantileSketch empty = new SummarizedQuantileSketch(ACCURACY);
		Assertions.assertTrue(empty.isEmpty());
		Assertions.assertEquals(0.0, empty.sum(), 0.0);
		Assertions.assertThrows(NoSuchElementException.class, empty::average);
		Assertions.assertThrows(NoSuchElementException.class, empty::min);
		Assertions.assertThrows(NoSuchElementException.class, empty::max);
		Assertions.assertThrows(NoSuchElementException.class, () -> empty.quantile(0.5));

		// merging an empty sketch into a populated one must not disturb min or max
		SummarizedQuantileSketch populated = new SummarizedQuantileSketch(ACCURACY);
		populated.accept(3);
		populated.mergeWith(empty);
		populated.mergeFrom(empty.toBytes());
		Assertions.assertEquals(1, populated.count());
		Assertions.assertEquals(3, populated.min(), 0.0);
		Assertions.assertEquals(3, populated.max(), 0.0);
		assertSameSketch(empty, SummarizedQuantileSketch.fromBytes(empty.toBytes()));
	}

	@Test
	void rejectedValuesAndMergesLeaveTheSummaryAlone() {
		SummarizedQuantileSketch sketch = new SummarizedQuantileSketch(ACCURACY);
		sketch.accept(2);
		Assertions.assertThrows(IllegalArgumentException.class, () -> sketch.accept(Double.NaN));
		Assertions.assertThrows(IllegalArgumentException.class, () -> sketch.accept(Double.POSITIVE_INFINITY));

		SummarizedQuantileSketch otherShape = new SummarizedQuantileSketch(ACCURACY * 2);
		otherShape.accept(-1000);
		Assertions.assertThrows(IllegalArgumentException.class, () -> sketch.mergeWith(otherShape));
		Assertions.assertThrows(IllegalArgumentException.class, () -> sketch.mergeFrom(otherShape.toBytes()));

		Assertions.assertEquals(1, sketch.count());
		Assertions.assertEquals(2, sketch.sum(), 0.0);
		Assertions.assertEquals(2, sketch.min(), 0.0);
		Assertions.assertEquals(2, sketch.max(), 0.0);
	}

	@Test
	void bytesUseTheReleasedLayout() {
		SummarizedQuantileSketch summarized = new SummarizedQuantileSketch(ACCURACY);
		QuantileSketch plain = new QuantileSketch(ACCURACY);
		for (double value : new double[] { 3, -2, 0, 1000.5 }) {
			summarized.accept(value);
			plain.accept(value);
		}

		// the 5.6.0 layout: version 1, mapping, accuracy, then sum, min and max as little endian doubles, then the bins
		// both kinds write it, a plain sketch with the sum, min and max of no values
		byte[] plainBytes = plain.toBytes();
		Assertions.assertEquals(1, plainBytes[0]);
		ByteBuffer plainSlots = ByteBuffer.wrap(plainBytes, 10, 24).order(ByteOrder.LITTLE_ENDIAN);
		Assertions.assertEquals(0.0, plainSlots.getDouble(), 0.0);
		Assertions.assertEquals(Double.POSITIVE_INFINITY, plainSlots.getDouble(), 0.0);
		Assertions.assertEquals(Double.NEGATIVE_INFINITY, plainSlots.getDouble(), 0.0);

		byte[] released = plainBytes.clone();
		ByteBuffer.wrap(released, 10, 24).order(ByteOrder.LITTLE_ENDIAN).putDouble(summarized.sum()).putDouble(summarized.min())
				.putDouble(summarized.max());
		Assertions.assertArrayEquals(released, summarized.toBytes());

		// a plain sketch reads summarized bytes and skips the exact numbers
		QuantileSketch fromSummarized = QuantileSketch.fromBytes(summarized.toBytes());
		Assertions.assertEquals(plain.count(), fromSummarized.count());
		Assertions.assertArrayEquals(plain.quantiles(QUANTILES), fromSummarized.quantiles(QUANTILES), 0.0);
		fromSummarized.mergeFrom(summarized.toBytes());
		Assertions.assertEquals(2 * plain.count(), fromSummarized.count());
	}

	@Test
	void plainBytesHaveNoSummaryToRead() {
		QuantileSketch plain = new QuantileSketch(ACCURACY);
		plain.accept(3);
		IllegalArgumentException fromBytes = Assertions.assertThrows(IllegalArgumentException.class,
				() -> SummarizedQuantileSketch.fromBytes(plain.toBytes()));
		Assertions.assertTrue(fromBytes.getMessage().contains("plain QuantileSketch bytes"), fromBytes.getMessage());

		SummarizedQuantileSketch sketch = new SummarizedQuantileSketch(ACCURACY);
		sketch.accept(5);
		Assertions.assertThrows(IllegalArgumentException.class, () -> sketch.mergeFrom(plain.toBytes()));
		Assertions.assertEquals(1, sketch.count());
		Assertions.assertEquals(5, sketch.sum(), 0.0);

		// an empty plain sketch's slots are exactly the summary of no values
		SummarizedQuantileSketch empty = SummarizedQuantileSketch.fromBytes(new QuantileSketch(ACCURACY).toBytes());
		Assertions.assertTrue(empty.isEmpty());
		Assertions.assertEquals(0, empty.sum(), 0.0);
		sketch.mergeFrom(new QuantileSketch(ACCURACY).toBytes());
		Assertions.assertEquals(1, sketch.count());
		Assertions.assertEquals(5, sketch.min(), 0.0);

		byte[] unknown = sketch.toBytes();
		unknown[0] = 2;
		IllegalArgumentException version = Assertions.assertThrows(IllegalArgumentException.class, () -> SummarizedQuantileSketch.fromBytes(unknown));
		Assertions.assertTrue(version.getMessage().contains("version 2"), version.getMessage());
	}

	@Test
	void failedMergeFromLeavesTheSketchUntouched() {
		SummarizedQuantileSketch source = new SummarizedQuantileSketch(ACCURACY);
		for (double value : randomValues(new Random(29), 2000)) {
			source.accept(value);
		}
		byte[] bytes = source.toBytes();
		byte[] truncated = Arrays.copyOf(bytes, bytes.length - 40);

		SummarizedQuantileSketch sketch = new SummarizedQuantileSketch(ACCURACY);
		sketch.accept(7);
		Assertions.assertThrows(RuntimeException.class, () -> sketch.mergeFrom(truncated));
		Assertions.assertEquals(1, sketch.count());
		Assertions.assertEquals(7, sketch.sum(), 0.0);
		Assertions.assertEquals(7, sketch.quantile(1), 7 * ACCURACY);
	}

	private static void assertSameSketch(SummarizedQuantileSketch expected, SummarizedQuantileSketch actual) {
		Assertions.assertEquals(expected.count(), actual.count(), "count");
		Assertions.assertEquals(expected.bins(), actual.bins(), "bins");
		Assertions.assertEquals(expected.sum(), actual.sum(), 0.0, "sum");
		if (!expected.isEmpty()) {
			Assertions.assertEquals(expected.min(), actual.min(), 0.0, "min");
			Assertions.assertEquals(expected.max(), actual.max(), 0.0, "max");
			Assertions.assertArrayEquals(expected.quantiles(QUANTILES), actual.quantiles(QUANTILES), 0.0, "quantiles");
		}
	}

	private static double[] randomValues(Random random, int count) {
		double[] values = new double[count];
		for (int i = 0; i < count; i++) {
			double magnitude = Math.exp(random.nextGaussian() * 3);
			values[i] = random.nextInt(10) == 0 ? -magnitude : magnitude;
		}
		return values;
	}
}
