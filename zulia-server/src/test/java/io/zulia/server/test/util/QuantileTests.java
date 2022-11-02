package io.zulia.server.test.util;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.DDSketches;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class QuantileTests {
	public static void main(String[] args) {
		// Creating an initially empty sketch, with low memory footprint
		double relativeAccuracy = 0.001;
		DDSketch sketch = DDSketches.unboundedDense(relativeAccuracy);
		System.out.println("Building: ");
		long startTime = System.nanoTime();
		populateData(50000.0, 800, sketch);

		// Querying the sketch
		System.out.println("Sketch: ");
		System.out.println(sketch.getValueAtQuantile(0.5)); // returns the median value
		System.out.println(sketch.getMinValue());
		System.out.println(sketch.getMaxValue());
		long endTime = System.nanoTime();
		System.out.println("Duration: " + (endTime - startTime) / 1E+9 + "s");

		// Merging another sketch into the sketch, in-place
		System.out.println("Building another...");
		DDSketch anotherSketch = DDSketches.unboundedDense(relativeAccuracy);
		populateData(25000.0, 1E+6, anotherSketch);
		System.out.println("Merging...");
		sketch.mergeWith(anotherSketch);
		System.out.println("Sketch 2: ");
		System.out.println(sketch.getValueAtQuantile(0.1)); // returns the median value
		System.out.println(sketch.getValueAtQuantile(0.2)); // returns the median value
		System.out.println(sketch.getValueAtQuantile(0.3)); // returns the median value
		System.out.println(sketch.getValueAtQuantile(0.4)); // returns the median value
		System.out.println(sketch.getValueAtQuantile(0.5)); // returns the median value
		System.out.println(sketch.getValueAtQuantile(0.6)); // returns the median value
		System.out.println(sketch.getValueAtQuantile(0.7)); // returns the median value
		System.out.println(sketch.getValueAtQuantile(0.8)); // returns the median value
		System.out.println(sketch.getValueAtQuantile(0.9)); // returns the median value
		System.out.println(sketch.getMinValue());
		System.out.println(sketch.getMaxValue());
		System.out.println(sketch.serializedSize());
	}

	public static List<Double> getValueArray(Double max, double numValues) {
		Random rd = new Random();
		List<Double> values = new ArrayList<>();
		for (long i = 0; i < numValues; i++) {
			values.add(rd.nextDouble() * max);
		}
		return values;
	}

	public static void populateData(Double max, double numValues, DDSketch sketch) {
		Random rd = new Random();
		for (long i = 0; i < numValues; i++) {
			sketch.accept(i);
			sketch.accept(1);
			if (i % 10000000 == 0) {
				System.out.println((i / numValues) * 100 + "%");
			}
		}
	}

}
