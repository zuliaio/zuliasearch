package io.zulia.util.sketch;

import io.zulia.util.sketch.QuantileSketch.Mapping;

import java.util.NoSuchElementException;

/**
 * The bins, quantiles and byte form that {@link QuantileSketch} and {@link SummarizedQuantileSketch} share. Each of them
 * writes its own per value accept, and neither extends the other, so each merges only with its own type.
 */
abstract sealed class SketchBins permits QuantileSketch, SummarizedQuantileSketch {

	private static final double A = 6.0 / 35.0;
	private static final double B = -3.0 / 5.0;
	private static final double C = 10.0 / 7.0;
	private static final long SIGNIFICAND_MASK = 0x000fffffffffffffL;
	private static final long EXPONENT_MASK = 0x7FF0000000000000L;
	private static final long ONE = 0x3ff0000000000000L;

	private final Mapping mapping;
	private final double requestedAccuracy;
	private final double relativeAccuracy;
	private final double gamma;
	private final double multiplier;
	private final double minIndexable;
	private final double maxIndexable;
	private final int linearDigits;
	private final int linearShift;
	private final int linearMultiplier;

	private final BinCounts positive = new BinCounts();
	private final BinCounts negative = new BinCounts();
	private long zeroCount;

	SketchBins(double relativeAccuracy, Mapping mapping) {
		if (!(relativeAccuracy > 0 && relativeAccuracy < 1)) {
			throw new IllegalArgumentException("Sketch relative accuracy must be between 0 and 1 exclusive, got " + relativeAccuracy);
		}
		this.mapping = mapping;
		this.requestedAccuracy = relativeAccuracy;
		if (mapping == Mapping.LINEAR) {
			double m = 1 / Math.log1p(2 * relativeAccuracy / (1 - relativeAccuracy));
			this.linearDigits = Math.max((int) Math.ceil(Math.log(m) / Math.log(2)), 0);
			this.linearShift = 53 - linearDigits - 1;
			this.linearMultiplier = 1 << linearDigits;
			this.gamma = Math.pow(2, 1.0 / linearMultiplier);
			this.multiplier = linearMultiplier;
			this.relativeAccuracy = 1 - 2 / (1 + Math.exp(1.0 / linearMultiplier));
			this.minIndexable = Math.max(Math.pow(2, Integer.MIN_VALUE / multiplier + 1),
					Double.MIN_NORMAL * (1 + this.relativeAccuracy) / (1 - this.relativeAccuracy));
			this.maxIndexable = Math.min(Math.pow(2, Integer.MAX_VALUE / multiplier - 1), Double.MAX_VALUE / (1 + this.relativeAccuracy));
			return;
		}
		this.linearDigits = 0;
		this.linearShift = 0;
		this.linearMultiplier = 0;
		double correctingFactor = mapping == Mapping.LOG ? 1 : 1 / (C * Math.log(2));
		double base = mapping == Mapping.LOG ? Math.E : 2;
		double exactLogGamma = (1 + relativeAccuracy) / (1 - relativeAccuracy);
		this.gamma = Math.pow(exactLogGamma, 1 / correctingFactor);
		this.multiplier = Math.log(base) / Math.log1p(gamma - 1);
		double corrected = Math.pow(gamma, correctingFactor);
		this.relativeAccuracy = (corrected - 1) / (corrected + 1);
		this.minIndexable = Math.max(Math.pow(base, Integer.MIN_VALUE / multiplier + 1),
				Double.MIN_NORMAL * (1 + this.relativeAccuracy) / (1 - this.relativeAccuracy));
		this.maxIndexable = Math.min(Math.pow(base, Integer.MAX_VALUE / multiplier - 1), Double.MAX_VALUE / (1 + this.relativeAccuracy));
	}

	public Mapping mapping() {
		return mapping;
	}

	public double relativeAccuracy() {
		return relativeAccuracy;
	}

	/** The bin update for one value, each sketch's accept calls it. */
	final void addToBins(double value) {
		// the hot path stays small so the JIT inlines it into the callers, every failure is thrown from a cold method
		if (value > minIndexable) {
			if (value > maxIndexable) {
				throw tooLarge(value);
			}
			positive.add(index(value));
		}
		else if (value < -minIndexable) {
			if (-value > maxIndexable) {
				throw tooSmall(value);
			}
			negative.add(index(-value));
		}
		else {
			// NaN fails both comparisons above and lands here
			if (value != value) {
				throw notFinite(value);
			}
			zeroCount++;
		}
	}

	private static IllegalArgumentException notFinite(double value) {
		return new IllegalArgumentException("Cannot add " + value + " to a sketch, only finite values can be sketched");
	}

	private IllegalArgumentException tooLarge(double value) {
		if (Double.isInfinite(value)) {
			return notFinite(value);
		}
		return new IllegalArgumentException("Cannot add " + value + " to a sketch, it is above the largest indexable value " + maxIndexable);
	}

	private IllegalArgumentException tooSmall(double value) {
		if (Double.isInfinite(value)) {
			return notFinite(value);
		}
		return new IllegalArgumentException("Cannot add " + value + " to a sketch, it is below the smallest indexable value " + -maxIndexable);
	}

	private int index(double value) {
		// a field compare instead of an enum switch, which looks the ordinal up through the switch map on every value
		if (mapping == Mapping.CUBIC) {
			return cubicIndex(value);
		}
		if (mapping == Mapping.LOG) {
			return floorIndex(Math.log(value) * multiplier);
		}
		return linearIndex(value);
	}

	private int cubicIndex(double value) {
		long bits = Double.doubleToRawLongBits(value);
		double s = Double.longBitsToDouble((bits & SIGNIFICAND_MASK) | ONE) - 1;
		double e = (double) (((bits & EXPONENT_MASK) >> 52) - 1023);
		return floorIndex((((A * s + B) * s + C) * s + e) * multiplier);
	}

	private int linearIndex(double value) {
		long bits = Double.doubleToRawLongBits(value);
		long exponent = ((bits & EXPONENT_MASK) >> 52) - 1023;
		return (int) ((exponent << linearDigits) | ((bits & SIGNIFICAND_MASK) >> linearShift));
	}

	private static int floorIndex(double index) {
		return index >= 0 ? (int) index : (int) index - 1;
	}

	private double value(int index) {
		if (mapping == Mapping.LINEAR) {
			int exponent = Math.floorDiv(index, linearMultiplier);
			double significandPlusOne = 1 - exponent + (double) index / linearMultiplier;
			double lower = Double.longBitsToDouble(
					((((long) exponent + 1023) << 52) & EXPONENT_MASK) | (Double.doubleToRawLongBits(significandPlusOne) & SIGNIFICAND_MASK));
			return lower * (1 + relativeAccuracy);
		}
		double x = index / multiplier;
		double lower;
		if (mapping == Mapping.LOG) {
			lower = Math.exp(x);
		}
		else {
			long exponent = (long) Math.floor(x);
			double d0 = B * B - 3 * A * C;
			double d1 = 2 * B * B * B - 9 * A * B * C - 27 * A * A * (x - exponent);
			double p = Math.cbrt((d1 - Math.sqrt(d1 * d1 - 4 * d0 * d0 * d0)) / 2);
			double significandPlusOne = -(B + p + d0 / p) / (3 * A) + 1;
			lower = Double.longBitsToDouble((((exponent + 1023) << 52) & EXPONENT_MASK) | (Double.doubleToRawLongBits(significandPlusOne) & SIGNIFICAND_MASK));
		}
		return lower * (1 + relativeAccuracy);
	}

	public long count() {
		return zeroCount + positive.total() + negative.total();
	}

	public boolean isEmpty() {
		return count() == 0;
	}

	/** Number of bins between the lowest and highest populated bin, including empty ones. */
	public int bins() {
		return positive.length() + negative.length();
	}

	/** Adds the other sketch's bins to this one, failing before any change when the shapes differ. */
	final void mergeBins(SketchBins other) {
		requireSameShape(other.mapping, other.requestedAccuracy);
		other.positive.verify();
		other.negative.verify();
		zeroCount += other.zeroCount;
		positive.merge(other.positive);
		negative.merge(other.negative);
	}

	/** The value at a quantile between 0 and 1, within the relative accuracy of the true value. */
	public double quantile(double quantile) {
		return quantiles(new double[] { quantile })[0];
	}

	/** Every requested quantile in one walk over the bins. The quantiles must be between 0 and 1 and ascending. */
	public double[] quantiles(double[] ascending) {
		requireValues("quantile");
		positive.verify();
		negative.verify();
		for (int i = 0; i < ascending.length; i++) {
			if (!(ascending[i] >= 0 && ascending[i] <= 1)) {
				throw new IllegalArgumentException("Quantile must be between 0 and 1, got " + ascending[i]);
			}
			if (i > 0 && ascending[i] < ascending[i - 1]) {
				throw new IllegalArgumentException("Quantiles must be ascending, got " + ascending[i] + " after " + ascending[i - 1]);
			}
		}
		double[] out = new double[ascending.length];
		long count = count();
		int q = 0;
		long n = 0;
		for (int i = negative.maxIndex(); q < out.length && negative.total() > 0 && i >= negative.minIndex(); i--) {
			n += negative.get(i);
			while (q < out.length && n > ascending[q] * (count - 1)) {
				out[q++] = -value(i);
			}
		}
		n += zeroCount;
		while (q < out.length && n > ascending[q] * (count - 1)) {
			out[q++] = 0;
		}
		for (int i = positive.minIndex(); q < out.length && positive.total() > 0 && i <= positive.maxIndex(); i++) {
			n += positive.get(i);
			while (q < out.length && n > ascending[q] * (count - 1)) {
				out[q++] = value(i);
			}
		}
		return out;
	}

	/** The byte form with the summarized sketch's exact numbers in the sum, min and max slots. */
	final byte[] toBytes(SketchSummary summary) {
		positive.verify();
		negative.verify();
		SketchByteWriter out = new SketchByteWriter(48 + 2 * bins());
		new SketchHeader(mapping, requestedAccuracy, summary).write(out);
		out.varLong(zeroCount);
		positive.write(out);
		negative.write(out);
		return out.toArray();
	}

	/** The mapping, accuracy and summary at the front of the bytes, so a reader can build a sketch of the right shape first. */
	static SketchHeader readHeader(byte[] bytes) {
		return SketchHeader.read(new SketchByteReader(bytes));
	}

	/** Adds the bins in the bytes to this sketch and returns the summary they carry, which is empty when a plain sketch wrote them. */
	final SketchSummary mergeBinsFrom(byte[] bytes) {
		SketchByteReader in = new SketchByteReader(bytes);
		SketchHeader sketchHeader = SketchHeader.read(in);
		requireSameShape(sketchHeader.mapping(), sketchHeader.requestedAccuracy());
		mergeBody(in);
		return sketchHeader.summary();
	}

	private void mergeBody(SketchByteReader in) {
		zeroCount += in.varLong();
		positive.mergeFrom(in);
		negative.mergeFrom(in);
	}

	final void requireValues(String what) {
		if (isEmpty()) {
			throw new NoSuchElementException("Cannot compute the " + what + " of a sketch with no values");
		}
	}

	private void requireSameShape(Mapping otherMapping, double otherAccuracy) {
		if (otherMapping != mapping || Double.doubleToLongBits(otherAccuracy) != Double.doubleToLongBits(requestedAccuracy)) {
			throw new IllegalArgumentException(
					"Cannot merge sketches with different bins: this sketch uses mapping " + mapping + " at relative accuracy " + requestedAccuracy
							+ " but the other uses mapping " + otherMapping + " at relative accuracy " + otherAccuracy);
		}
	}
}
