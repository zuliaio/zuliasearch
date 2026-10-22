package io.zulia.util.test;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.DDSketches;
import com.datadoghq.sketch.ddsketch.mapping.BitwiseLinearlyInterpolatedMapping;
import com.datadoghq.sketch.ddsketch.mapping.LogarithmicMapping;
import com.datadoghq.sketch.ddsketch.store.UnboundedSizeDenseStore;
import io.zulia.util.sketch.QuantileSketch;
import io.zulia.util.sketch.QuantileSketch.Mapping;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Random;

/** Each mapping must return the quantiles its Datadog library twin returns, so switching never moves a percentile. */
public class QuantileSketchLibraryEquivalenceTest {

	private static final double[] QUANTILES = { 0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1 };

	@ParameterizedTest
	@ValueSource(doubles = { 0.001, 0.01, 0.05 })
	void quantilesMatchTheLibrary(double accuracy) {
		Random random = new Random(17);
		for (int trial = 0; trial < 40; trial++) {
			DDSketch libraryCubic = DDSketches.unboundedDense(accuracy);
			DDSketch libraryLog = new DDSketch(new LogarithmicMapping(accuracy), UnboundedSizeDenseStore::new);
			DDSketch libraryLinear = new DDSketch(new BitwiseLinearlyInterpolatedMapping(accuracy), UnboundedSizeDenseStore::new);
			QuantileSketch cubic = new QuantileSketch(accuracy, Mapping.CUBIC);
			QuantileSketch log = new QuantileSketch(accuracy, Mapping.LOG);
			QuantileSketch linear = new QuantileSketch(accuracy, Mapping.LINEAR);

			double scale = Math.pow(10, random.nextInt(9) - 3);
			int sign = random.nextInt(3);
			int size = 1 + random.nextInt(5000);
			for (int i = 0; i < size; i++) {
				double value = Math.exp(random.nextGaussian()) * scale;
				if (sign == 1 || (sign == 2 && random.nextBoolean())) {
					value = -value;
				}
				if (random.nextInt(50) == 0) {
					value = 0;
				}
				libraryCubic.accept(value);
				libraryLog.accept(value);
				libraryLinear.accept(value);
				cubic.accept(value);
				log.accept(value);
				linear.accept(value);
			}

			assertSameQuantiles(libraryCubic, cubic, "cubic accuracy " + accuracy + " trial " + trial);
			assertSameQuantiles(libraryLog, log, "log accuracy " + accuracy + " trial " + trial);
			assertSameQuantiles(libraryLinear, linear, "linear accuracy " + accuracy + " trial " + trial);
			Assertions.assertEquals(libraryCubic.getCount(), cubic.count(), 0.0);
		}
	}

	private static void assertSameQuantiles(DDSketch library, QuantileSketch sketch, String message) {
		double[] quantiles = sketch.quantiles(QUANTILES);
		for (int i = 0; i < QUANTILES.length; i++) {
			double expected = library.getValueAtQuantile(QUANTILES[i]);
			Assertions.assertEquals(expected, quantiles[i], Math.abs(expected) * 1e-12, message + " quantile " + QUANTILES[i]);
		}
	}
}
