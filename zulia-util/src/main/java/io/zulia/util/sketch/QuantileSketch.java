package io.zulia.util.sketch;

import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * Quantile sketch with the DDSketch relative accuracy guarantee: every quantile it returns is within the relative
 * accuracy of the true quantile. Values are counted in exponentially sized bins. The default cubic mapping gives the
 * same bins and quantiles as the Datadog library's unbounded dense sketch, so quantiles do not move when switching.
 * Count, sum, min and max are exact. Counts are ints per bin. The byte form is a small header followed by varint bin
 * counts with runs of empty bins collapsed, and one sketch can be merged straight from another's bytes. Not thread safe.
 */
public final class QuantileSketch {

	/** How a value is mapped to a bin. Cubic is the default and the fastest of the exact-guarantee mappings. */
	public enum Mapping {
		/** Cubic interpolation of the logarithm, the library's default. */
		CUBIC(0),
		/** The true logarithm, slowest, fewest bins. */
		LOG(1),
		/** Bitwise linear interpolation, fastest, about 46 percent more bins, quantiles differ within the guarantee. */
		LINEAR(2);

		private final int code;

		Mapping(int code) {
			this.code = code;
		}

		private static Mapping fromCode(int code) {
			for (Mapping mapping : values()) {
				if (mapping.code == code) {
					return mapping;
				}
			}
			throw new IllegalArgumentException("Unknown sketch mapping code " + code + ", expected 0 (cubic), 1 (log) or 2 (linear)");
		}
	}

	private static final int FORMAT_VERSION = 1;

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

	private final Store positive = new Store();
	private final Store negative = new Store();
	private long zeroCount;
	private double sum;
	private double min = Double.POSITIVE_INFINITY;
	private double max = Double.NEGATIVE_INFINITY;

	/** A sketch with the cubic mapping. */
	public QuantileSketch(double relativeAccuracy) {
		this(relativeAccuracy, Mapping.CUBIC);
	}

	public QuantileSketch(double relativeAccuracy, Mapping mapping) {
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

	/** The guarantee actually provided, never worse than the accuracy requested. */
	public double relativeAccuracy() {
		return relativeAccuracy;
	}

	public void accept(double value) {
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
		sum += value;
		if (value < min) {
			min = value;
		}
		if (value > max) {
			max = value;
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
		return switch (mapping) {
			case CUBIC -> cubicIndex(value);
			case LOG -> floorIndex(Math.log(value) * multiplier);
			case LINEAR -> linearIndex(value);
		};
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
		return zeroCount + positive.total + negative.total;
	}

	public boolean isEmpty() {
		return count() == 0;
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

	/** Number of bins between the lowest and highest populated bin, including empty ones. */
	public int bins() {
		return positive.length() + negative.length();
	}

	public void mergeWith(QuantileSketch other) {
		requireSameShape(other.mapping, other.requestedAccuracy);
		zeroCount += other.zeroCount;
		positive.merge(other.positive);
		negative.merge(other.negative);
		mergeSummary(other.sum, other.min, other.max);
	}

	/** The value at a quantile between 0 and 1, within the relative accuracy of the true value. */
	public double quantile(double quantile) {
		return quantiles(new double[] { quantile })[0];
	}

	/** Every requested quantile in one walk over the bins. The quantiles must be between 0 and 1 and ascending. */
	public double[] quantiles(double[] ascending) {
		requireValues("quantile");
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
		for (int i = negative.maxIndex; q < out.length && negative.total > 0 && i >= negative.minIndex; i--) {
			n += negative.get(i);
			while (q < out.length && n > ascending[q] * (count - 1)) {
				out[q++] = -value(i);
			}
		}
		n += zeroCount;
		while (q < out.length && n > ascending[q] * (count - 1)) {
			out[q++] = 0;
		}
		for (int i = positive.minIndex; q < out.length && positive.total > 0 && i <= positive.maxIndex; i++) {
			n += positive.get(i);
			while (q < out.length && n > ascending[q] * (count - 1)) {
				out[q++] = value(i);
			}
		}
		return out;
	}

	public byte[] toBytes() {
		Out out = new Out(48 + 2 * bins());
		out.varLong(FORMAT_VERSION);
		out.varLong(mapping.code);
		out.fixedDouble(requestedAccuracy);
		out.fixedDouble(sum);
		out.fixedDouble(min);
		out.fixedDouble(max);
		out.varLong(zeroCount);
		positive.write(out);
		negative.write(out);
		return out.toArray();
	}

	/** Reads a sketch written by {@link #toBytes()}. */
	public static QuantileSketch fromBytes(byte[] bytes) {
		In in = new In(bytes);
		Header header = in.header();
		QuantileSketch sketch = new QuantileSketch(header.requestedAccuracy(), header.mapping());
		sketch.mergeBody(in, header);
		return sketch;
	}

	/** Adds a sketch written by {@link #toBytes()} straight into this one without building it first. */
	public void mergeFrom(byte[] bytes) {
		In in = new In(bytes);
		Header header = in.header();
		requireSameShape(header.mapping(), header.requestedAccuracy());
		mergeBody(in, header);
	}

	private void mergeBody(In in, Header header) {
		zeroCount += in.varLong();
		positive.mergeFrom(in);
		negative.mergeFrom(in);
		mergeSummary(header.sum(), header.min(), header.max());
	}

	private void mergeSummary(double otherSum, double otherMin, double otherMax) {
		sum += otherSum;
		min = Math.min(min, otherMin);
		max = Math.max(max, otherMax);
	}

	private void requireValues(String what) {
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

	private record Header(Mapping mapping, double requestedAccuracy, double sum, double min, double max) {
	}

	private static final class Store {
		private static final int CHUNK = 64;
		int[] counts = new int[0];
		int offset;
		int minIndex = Integer.MAX_VALUE;
		int maxIndex = Integer.MIN_VALUE;
		long total;

		int length() {
			return total == 0 ? 0 : maxIndex - minIndex + 1;
		}

		int get(int index) {
			return counts[index - offset];
		}

		void add(int index) {
			if (index < minIndex || index > maxIndex) {
				extend(Math.min(index, minIndex), Math.max(index, maxIndex));
			}
			if (++counts[index - offset] < 0) {
				throw overflow(index, 1, counts[index - offset] - 1);
			}
			total++;
		}

		void add(int index, int count) {
			if (index < minIndex || index > maxIndex) {
				extend(Math.min(index, minIndex), Math.max(index, maxIndex));
			}
			int combined = counts[index - offset] + count;
			if (combined < 0) {
				throw overflow(index, count, counts[index - offset]);
			}
			counts[index - offset] = combined;
			total += count;
		}

		private static ArithmeticException overflow(int index, int count, int existing) {
			return new ArithmeticException("Sketch bin " + index + " overflowed adding " + count + " to " + existing + " values");
		}

		private void extend(int newMin, int newMax) {
			int low = minIndex == Integer.MAX_VALUE ? newMin : Math.min(newMin, minIndex);
			int high = maxIndex == Integer.MIN_VALUE ? newMax : Math.max(newMax, maxIndex);
			if (counts.length == 0 || low < offset || high >= offset + counts.length) {
				int needed = high - low + 1;
				int size = needed + CHUNK; // grow to the range plus one chunk of slack, never geometrically
				int[] grown = new int[size];
				// leave room below when growing downward so the next lower index does not reallocate
				int pad = (counts.length == 0 || low < offset) ? Math.min(CHUNK / 2, size - needed) : 0;
				int newOffset = low - pad;
				if (minIndex <= maxIndex) {
					System.arraycopy(counts, minIndex - offset, grown, minIndex - newOffset, maxIndex - minIndex + 1);
				}
				counts = grown;
				offset = newOffset;
			}
			minIndex = low;
			maxIndex = high;
		}

		void merge(Store other) {
			if (other.total == 0) {
				return;
			}
			if (other.minIndex < minIndex || other.maxIndex > maxIndex) {
				extend(Math.min(other.minIndex, minIndex), Math.max(other.maxIndex, maxIndex));
			}
			for (int i = other.minIndex; i <= other.maxIndex; i++) {
				int count = other.counts[i - other.offset];
				if (count != 0) {
					add(i, count);
				}
			}
		}

		void write(Out out) {
			int length = length();
			out.varLong(length);
			if (length == 0) {
				return;
			}
			out.varLong(zigzag(minIndex));
			int i = minIndex;
			while (i <= maxIndex) {
				int c = counts[i - offset];
				if (c != 0) {
					out.varLong(c);
					i++;
				}
				else {
					int run = 0;
					while (i <= maxIndex && counts[i - offset] == 0) {
						run++;
						i++;
					}
					out.varLong(0);
					out.varLong(run);
				}
			}
		}

		void mergeFrom(In in) {
			int length = Math.toIntExact(in.varLong());
			if (length == 0) {
				return;
			}
			int first = Math.toIntExact(unzigzag(in.varLong()));
			int last = first + length - 1;
			if (first < minIndex || last > maxIndex) {
				extend(Math.min(first, minIndex), Math.max(last, maxIndex));
			}
			int i = first;
			while (i <= last) {
				int c = Math.toIntExact(in.varLong());
				if (c != 0) {
					add(i, c);
					i++;
				}
				else {
					i += Math.toIntExact(in.varLong());
				}
			}
		}
	}

	private static long zigzag(long v) {
		return (v << 1) ^ (v >> 63);
	}

	private static long unzigzag(long v) {
		return (v >>> 1) ^ -(v & 1);
	}

	private static final class Out {
		byte[] bytes;
		int n;

		Out(int capacity) {
			bytes = new byte[Math.max(16, capacity)];
		}

		void varLong(long v) {
			ensure(10);
			while ((v & ~0x7FL) != 0) {
				bytes[n++] = (byte) ((v & 0x7F) | 0x80);
				v >>>= 7;
			}
			bytes[n++] = (byte) v;
		}

		void fixedDouble(double v) {
			ensure(8);
			long bits = Double.doubleToRawLongBits(v);
			for (int i = 0; i < 8; i++) {
				bytes[n++] = (byte) (bits >>> (8 * i));
			}
		}

		private void ensure(int room) {
			if (n + room > bytes.length) {
				bytes = Arrays.copyOf(bytes, Math.max(bytes.length * 2, n + room));
			}
		}

		byte[] toArray() {
			return Arrays.copyOf(bytes, n);
		}
	}

	private static final class In {
		final byte[] bytes;
		int n;

		In(byte[] bytes) {
			this.bytes = bytes;
		}

		Header header() {
			long version = varLong();
			if (version != FORMAT_VERSION) {
				throw new IllegalArgumentException("Unsupported sketch format version " + version + ", this build reads version " + FORMAT_VERSION);
			}
			Mapping mapping = Mapping.fromCode(Math.toIntExact(varLong()));
			double requestedAccuracy = fixedDouble();
			double sum = fixedDouble();
			double min = fixedDouble();
			double max = fixedDouble();
			return new Header(mapping, requestedAccuracy, sum, min, max);
		}

		long varLong() {
			long v = 0;
			int shift = 0;
			while (true) {
				byte b = bytes[n++];
				v |= (long) (b & 0x7F) << shift;
				if (b >= 0) {
					return v;
				}
				shift += 7;
			}
		}

		double fixedDouble() {
			long bits = 0;
			for (int i = 0; i < 8; i++) {
				bits |= (bytes[n++] & 0xFFL) << (8 * i);
			}
			return Double.longBitsToDouble(bits);
		}
	}
}
