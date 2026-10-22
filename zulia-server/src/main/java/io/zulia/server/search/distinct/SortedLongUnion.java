package io.zulia.server.search.distinct;

import org.apache.lucene.search.TaskExecutor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Unions ascending long arrays by merging them pairwise, level by level, with the pairs of levels in parallel. The
 * inputs are read in place, the levels alternate between two buffers, and a count needs neither the last write nor
 * the final copy.
 */
final class SortedLongUnion {

	private SortedLongUnion() {
	}

	/**
	 * A stretch of ascending values: positions offset to offset + length of source.
	 */
	record Run(long[] source, int offset, int length) {

		static Run of(long[] array) {
			return new Run(array, 0, array.length);
		}

		int end() {
			return offset + length;
		}

		boolean isWhole() {
			return offset == 0 && length == source.length;
		}
	}

	/**
	 * The sorted union of the arrays, each shared value once.
	 */
	static long[] union(List<long[]> sortedArrays, TaskExecutor taskExecutor) throws IOException {
		Run[] runs = runs(sortedArrays);
		if (runs.length == 0) {
			return new long[0];
		}
		Buffers buffers = new Buffers(total(runs));
		while (runs.length > 1) {
			runs = mergeLevel(runs, buffers, taskExecutor);
		}
		Run result = runs[0];
		return result.isWhole() ? result.source() : Arrays.copyOfRange(result.source(), result.offset(), result.end());
	}

	/**
	 * The number of distinct values across the arrays, without writing out the final union.
	 */
	static long countDistinct(List<long[]> sortedArrays, TaskExecutor taskExecutor) throws IOException {
		Run[] runs = runs(sortedArrays);
		// The first level is the inputs' last use. Dropping this frame's reference lets a caller that passed a
		// temporary list have them collected while the later levels run
		sortedArrays = null;
		if (runs.length <= 1) {
			return runs.length == 0 ? 0 : runs[0].length();
		}
		Buffers buffers = new Buffers(total(runs));
		while (runs.length > 2) {
			runs = mergeLevel(runs, buffers, taskExecutor);
		}
		return countMerged(runs[0], runs[1]);
	}

	private static Run[] runs(List<long[]> sortedArrays) {
		return sortedArrays.stream().filter(array -> array.length > 0).map(Run::of).toArray(Run[]::new);
	}

	private static int total(Run[] runs) {
		long total = 0;
		for (Run run : runs) {
			total += run.length();
		}
		if (total > Integer.MAX_VALUE) {
			throw new IllegalStateException("Sorted arrays total " + total + " values, more than one merge buffer can hold");
		}
		return (int) total;
	}

	/**
	 * Merges adjacent pairs of runs into the next buffer. An odd run is carried, copied only when a later level would overwrite it.
	 */
	private static Run[] mergeLevel(Run[] runs, Buffers buffers, TaskExecutor taskExecutor) throws IOException {
		long[] target = buffers.next();
		List<Callable<Run>> level = new ArrayList<>(runs.length / 2 + 1);
		int out = 0;
		for (int i = 0; i + 1 < runs.length; i += 2) {
			Run a = runs[i];
			Run b = runs[i + 1];
			int offset = out;
			level.add(() -> mergeInto(a, b, target, offset));
			out += a.length() + b.length();
		}
		if (runs.length % 2 == 1) {
			Run odd = runs[runs.length - 1];
			int offset = out;
			// an input array is never written, but the other buffer is the next level's target
			level.add(buffers.holds(odd.source()) ? () -> copyInto(odd, target, offset) : () -> odd);
		}
		Run[] merged = taskExecutor.invokeAll(level).toArray(Run[]::new);
		// Later levels never exceed this level's output, so the next buffer needs only be this big
		buffers.nextNeedsAtMost(total(merged));
		return merged;
	}

	/**
	 * Writes the union of two runs into {@code to} at {@code offset}, which has room for both.
	 */
	static Run mergeInto(Run a, Run b, long[] to, int offset) {
		return new Run(to, offset, merge(a, b, to, offset, a.length() + b.length()));
	}

	/**
	 * The {@code limit} smallest values of the union of two ascending arrays, each shared value once.
	 */
	static long[] smallestUnion(long[] a, long[] b, int limit) {
		return smallestUnion(Run.of(a), Run.of(b), limit);
	}

	/**
	 * The {@code limit} smallest values of the union of two ascending runs, each shared value once.
	 */
	static long[] smallestUnion(Run a, Run b, int limit) {
		long[] to = new long[(int) Math.min(limit, (long) a.length() + b.length())];
		int length = merge(a, b, to, 0, to.length);
		return length == to.length ? to : Arrays.copyOf(to, length);
	}

	/**
	 * Writes the first {@code room} values of the union of two runs into {@code to} at {@code offset}, returning how many it wrote.
	 */
	private static int merge(Run a, Run b, long[] to, int offset, int room) {
		long[] fromA = a.source();
		long[] fromB = b.source();
		int nextA = a.offset();
		int nextB = b.offset();
		int out = offset;
		int end = offset + room;
		while (out < end && nextA < a.end() && nextB < b.end()) {
			long x = fromA[nextA];
			long y = fromB[nextB];
			if (x < y) {
				to[out++] = x;
				nextA++;
			}
			else if (y < x) {
				to[out++] = y;
				nextB++;
			}
			else {
				to[out++] = x;
				nextA++;
				nextB++;
			}
		}
		// one side is spent or the room is full, so whatever fits comes straight from the other side
		out = nextA < a.end() ? copyTail(fromA, nextA, a.end(), to, out, end) : copyTail(fromB, nextB, b.end(), to, out, end);
		return out - offset;
	}

	/**
	 * Copies as much of {@code from} between {@code next} and {@code stop} as fits before {@code end}, returning the new out.
	 */
	private static int copyTail(long[] from, int next, int stop, long[] to, int out, int end) {
		int length = Math.min(stop - next, end - out);
		System.arraycopy(from, next, to, out, length);
		return out + length;
	}

	/**
	 * The size the union of two runs would have, without writing it.
	 */
	static long countMerged(Run a, Run b) {
		long[] fromA = a.source();
		long[] fromB = b.source();
		int nextA = a.offset();
		int nextB = b.offset();
		long count = 0;
		while (nextA < a.end() && nextB < b.end()) {
			long x = fromA[nextA];
			long y = fromB[nextB];
			if (x < y) {
				nextA++;
			}
			else if (y < x) {
				nextB++;
			}
			else {
				nextA++;
				nextB++;
			}
			count++;
		}
		return count + (a.end() - nextA) + (b.end() - nextB);
	}

	private static Run copyInto(Run run, long[] to, int offset) {
		System.arraycopy(run.source(), run.offset(), to, offset, run.length());
		return new Run(to, offset, run.length());
	}

	/**
	 * Two merge buffers handed out in turn. The first holds the whole input, since a pair's merged size is only known
	 * after it merges. The second is allocated once the first level has run and is sized to its output.
	 */
	private static final class Buffers {
		private int size;
		private long[] first;
		private long[] second;
		private boolean useFirst = true;

		Buffers(int size) {
			this.size = size;
		}

		long[] next() {
			long[] buffer;
			if (useFirst) {
				buffer = first == null ? (first = new long[size]) : first;
			}
			else {
				buffer = second == null ? (second = new long[size]) : second;
			}
			useFirst = !useFirst;
			return buffer;
		}

		void nextNeedsAtMost(int needed) {
			size = Math.min(size, needed);
		}

		boolean holds(long[] array) {
			return array == first || array == second;
		}
	}
}
