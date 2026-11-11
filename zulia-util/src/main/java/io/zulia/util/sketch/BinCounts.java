package io.zulia.util.sketch;

/**
 * The value counts for one run of neighboring bins, in an int array that grows to span the lowest and highest bin index
 * seen. A sketch keeps one for its positive values and one for its negative values.
 */
final class BinCounts {

	private static final int CHUNK = 64;

	private int[] counts = new int[0];
	private int offset;
	private int minIndex = Integer.MAX_VALUE;
	private int maxIndex = Integer.MIN_VALUE;
	private long total;

	/** Number of values counted across all bins. */
	long total() {
		return total;
	}

	/** Lowest bin index that has held a value, only meaningful when the total is above zero. */
	int minIndex() {
		return minIndex;
	}

	/** Highest bin index that has held a value, only meaningful when the total is above zero. */
	int maxIndex() {
		return maxIndex;
	}

	/** Number of bins between the lowest and highest populated bin, including empty ones. */
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
		// no overflow compare per value, a wrapped bin is caught by verify() before the counts are read
		counts[index - offset]++;
		total++;
	}

	/** A bin can only wrap after more than Integer.MAX_VALUE values, so the exact total against the bin sum finds it. */
	void verify() {
		if (total <= Integer.MAX_VALUE) {
			return;
		}
		long binSum = 0;
		for (int i = minIndex; i <= maxIndex; i++) {
			binSum += counts[i - offset];
		}
		if (binSum != total) {
			throw new ArithmeticException(
					"Sketch bin counts overflowed: " + total + " values were added but the bins hold " + binSum + ", a bin holds at most "
							+ Integer.MAX_VALUE + " values");
		}
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

	void merge(BinCounts other) {
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

	/** Writes the bin span, the first bin index and one varint per bin, with a run of empty bins as a zero and the run length. */
	void write(SketchByteWriter out) {
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

	/** Adds the bins that {@link #write} wrote to the ones held here. */
	void mergeFrom(SketchByteReader in) {
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
			if (c < 0) {
				throw new IllegalArgumentException("Sketch bytes hold a negative count " + c + " for bin " + i + ", bin counts are never negative");
			}
			if (c != 0) {
				add(i, c);
				i++;
			}
			else {
				i += Math.toIntExact(in.varLong());
			}
		}
	}

	private static long zigzag(long v) {
		return (v << 1) ^ (v >> 63);
	}

	private static long unzigzag(long v) {
		return (v >>> 1) ^ -(v & 1);
	}
}
