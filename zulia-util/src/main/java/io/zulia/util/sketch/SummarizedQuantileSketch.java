package io.zulia.util.sketch;

/**
 * A {@link QuantileSketch} that also keeps the exact sum, min and max of its values. It merges only with SummarizedQuantileSketch, so
 * the exact numbers always cover every value in the bins. Both kinds share the 5.6.0 QuantileSketch byte layout, and this one
 * fills the sum, min and max slots a plain sketch leaves empty, so it reads sketch bytes written by 5.6.0. Not thread safe.
 */
public final class SummarizedQuantileSketch extends SketchBins implements Quantiles {

	private double sum;
	private double min = Double.POSITIVE_INFINITY;
	private double max = Double.NEGATIVE_INFINITY;

	/** A sketch with the cubic mapping. */
	public SummarizedQuantileSketch(double relativeAccuracy) {
		this(relativeAccuracy, QuantileSketch.Mapping.CUBIC);
	}

	public SummarizedQuantileSketch(double relativeAccuracy, QuantileSketch.Mapping mapping) {
		super(relativeAccuracy, mapping);
	}

	@Override
	public void accept(double value) {
		// the bins reject values they cannot hold before the exact numbers move
		addToBins(value);
		sum += value;
		if (value < min) {
			min = value;
		}
		if (value > max) {
			max = value;
		}
	}

	/** Exact sum of the values added. */
	public double sum() {
		return sum;
	}

	/** Exact average of the values added. */
	public double average() {
		requireValues("average");
		return sum / count();
	}

	/** Exact smallest value added. */
	public double min() {
		requireValues("min");
		return min;
	}

	/** Exact largest value added. */
	public double max() {
		requireValues("max");
		return max;
	}

	/** Adds the other sketch's values to this one. */
	public void mergeWith(SummarizedQuantileSketch other) {
		// the bins first, a shape mismatch fails before the exact numbers move
		mergeBins(other);
		mergeSummary(other.summary());
	}

	public byte[] toBytes() {
		return toBytes(summary());
	}

	/** Reads a sketch written by {@link #toBytes()} or by a 5.6.0 QuantileSketch. */
	public static SummarizedQuantileSketch fromBytes(byte[] bytes) {
		SketchHeader sketchHeader = readHeader(bytes);
		SummarizedQuantileSketch sketch = new SummarizedQuantileSketch(sketchHeader.requestedAccuracy(), sketchHeader.mapping());
		SketchSummary summary = sketch.mergeBinsFrom(bytes);
		// a summary of real values always has min <= max, so values with an empty summary can only come from a plain sketch
		if (!sketch.isEmpty() && summary.coversNoValues()) {
			throw new IllegalArgumentException("These are plain QuantileSketch bytes holding " + sketch.count()
					+ " values with an empty sum, min and max. A SummarizedQuantileSketch can only read bytes that carry them");
		}
		sketch.mergeSummary(summary);
		return sketch;
	}

	/** Adds a sketch written by {@link #toBytes()} or by a 5.6.0 QuantileSketch to this one, all or nothing. */
	public void mergeFrom(byte[] bytes) {
		// read the bytes into a sketch of their own first, a merge that fails partway would leave the exact numbers short of the bins
		mergeWith(fromBytes(bytes));
	}

	private SketchSummary summary() {
		return new SketchSummary(sum, min, max);
	}

	private void mergeSummary(SketchSummary summary) {
		sum += summary.sum();
		min = Math.min(min, summary.min());
		max = Math.max(max, summary.max());
	}
}
