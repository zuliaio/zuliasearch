package io.zulia.server.search.distinct;

import org.apache.lucene.search.TaskExecutor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.LongStream;

public class SortedLongUnionTest {

	private static ExecutorService executor;

	@BeforeAll
	static void start() {
		executor = Executors.newVirtualThreadPerTaskExecutor();
	}

	@AfterAll
	static void stop() {
		executor.close();
	}

	@Test
	public void unionAndCountMatchASet() throws Exception {
		Random random = new Random(11);
		for (int trial = 0; trial < 200; trial++) {
			int arrayCount = random.nextInt(12);
			List<long[]> arrays = new ArrayList<>();
			Set<Long> expected = new HashSet<>();
			for (int s = 0; s < arrayCount; s++) {
				long[] keys = random.longs(random.nextInt(60), -40, 40).distinct().sorted().toArray();
				arrays.add(keys);
				for (long key : keys) {
					expected.add(key);
				}
			}
			TaskExecutor taskExecutor = new TaskExecutor(trial % 2 == 0 ? executor : Runnable::run);
			long[] sortedExpected = expected.stream().mapToLong(Long::longValue).sorted().toArray();
			Assertions.assertArrayEquals(sortedExpected, SortedLongUnion.union(arrays, taskExecutor), "trial " + trial);
			Assertions.assertEquals(sortedExpected.length, SortedLongUnion.countDistinct(arrays, taskExecutor), "trial " + trial);
		}
	}

	@Test
	public void edgesAndSingleInputs() throws Exception {
		TaskExecutor inline = new TaskExecutor(Runnable::run);
		Assertions.assertArrayEquals(new long[0], SortedLongUnion.union(List.of(), inline));
		Assertions.assertEquals(0, SortedLongUnion.countDistinct(List.of(), inline));
		Assertions.assertEquals(0, SortedLongUnion.countDistinct(List.of(new long[0], new long[0]), inline));
		long[] only = { 1, 2, 3 };
		Assertions.assertSame(only, SortedLongUnion.union(List.of(new long[0], only), inline), "a single input is returned as is");
		Assertions.assertEquals(3, SortedLongUnion.countDistinct(List.of(only, new long[0]), inline));
	}

	@Test
	public void smallestUnionKeepsTheLowestValuesOnce() {
		Random random = new Random(13);
		for (int trial = 0; trial < 200; trial++) {
			long[] a = random.longs(random.nextInt(40), -30, 30).distinct().sorted().toArray();
			long[] b = random.longs(random.nextInt(40), -30, 30).distinct().sorted().toArray();
			int limit = 1 + random.nextInt(50);
			long[] expected = LongStream.concat(Arrays.stream(a), Arrays.stream(b)).distinct().sorted().limit(limit).toArray();
			Assertions.assertArrayEquals(expected, SortedLongUnion.smallestUnion(a, b, limit), "trial " + trial);
		}
		Assertions.assertArrayEquals(new long[0], SortedLongUnion.smallestUnion(new long[0], new long[0], 5));
		Assertions.assertArrayEquals(new long[] { 1, 2 }, SortedLongUnion.smallestUnion(new long[] { 1, 4 }, new long[] { 1, 2, 3 }, 2));
	}

	@Test
	public void mergeIntoAndCountMergedAgree() {
		long[] buffer = { 1, 3, 5, 3, 4, 5, 9 };
		SortedLongUnion.Run a = new SortedLongUnion.Run(buffer, 0, 3);
		SortedLongUnion.Run b = new SortedLongUnion.Run(buffer, 3, 4);
		long[] out = new long[9];
		SortedLongUnion.Run union = SortedLongUnion.mergeInto(a, b, out, 2);
		Assertions.assertEquals(new SortedLongUnion.Run(out, 2, 5), union);
		Assertions.assertArrayEquals(new long[] { 1, 3, 4, 5, 9 }, Arrays.copyOfRange(out, 2, 7));
		Assertions.assertEquals(5, SortedLongUnion.countMerged(a, b));
	}
}
