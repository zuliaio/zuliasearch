package io.zulia.server.search.distinct;

import org.apache.lucene.util.FixedBitSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.LongStream;

public class DistinctValueCollectorTest {

	@Test
	public void stringsKeepOneBitSetPerLeaf() {
		StringDistinctValueCollector ordinals = new StringDistinctValueCollector("f");
		FixedBitSet first = ordinals.forLeaf(0, 10);
		first.set(3);
		first.set(7);
		Assertions.assertSame(first, ordinals.forLeaf(0, 10), "the same leaf reuses its bit set");
		Assertions.assertEquals(10, first.length());
		ordinals.forLeaf(2, 4).set(1);
		Assertions.assertEquals(2, ordinals.ordsByLeaf().size());
		Assertions.assertEquals(2, ordinals.ordsByLeaf().get(0).cardinality());
	}

	@Test
	public void stringsMergeAcrossSlices() {
		StringDistinctValueCollector a = new StringDistinctValueCollector("f");
		StringDistinctValueCollector b = new StringDistinctValueCollector("f");
		a.forLeaf(0, 8).set(1);
		b.forLeaf(0, 8).set(5);
		b.forLeaf(1, 8).set(2);
		a.merge(b);
		Assertions.assertEquals(2, a.ordsByLeaf().size());
		Assertions.assertTrue(a.ordsByLeaf().get(0).get(1) && a.ordsByLeaf().get(0).get(5), "a leaf seen by two slices is the union of both");
		Assertions.assertTrue(a.ordsByLeaf().get(1).get(2));
	}

	@Test
	public void numericsCountEachKeyOnceIncludingZero() {
		NumericDistinctValueCollector longs = new NumericDistinctValueCollector("f", 100);
		for (long key : new long[] { 0, 0, -5, 2015, 2015, Long.MAX_VALUE }) {
			longs.add(key);
		}
		Assertions.assertEquals(4, longs.count());
		Assertions.assertFalse(longs.isCapped());
		Assertions.assertArrayEquals(hashes(0, -5, 2015, Long.MAX_VALUE), longs.smallestHashes());
	}

	@Test
	public void numericsKeepTheCapSmallestHashesPastTheCap() {
		NumericDistinctValueCollector longs = new NumericDistinctValueCollector("f", 2);
		for (long key = 2015; key < 2020; key++) {
			longs.add(key);
			longs.add(key); // a repeat never counts as a drop
		}
		Assertions.assertTrue(longs.isCapped());
		Assertions.assertEquals(3, longs.count(), "one past the cap is the floor");
		Assertions.assertArrayEquals(Arrays.copyOf(hashes(2015, 2016, 2017, 2018, 2019), 2), longs.smallestHashes());
	}

	@Test
	public void numericsMergeAcrossSlices() {
		NumericDistinctValueCollector a = new NumericDistinctValueCollector("f", 2);
		NumericDistinctValueCollector b = new NumericDistinctValueCollector("f", 2);
		a.add(1);
		b.add(2);
		b.add(3);
		a.merge(b);
		Assertions.assertTrue(a.isCapped());
		Assertions.assertEquals(3, a.count());
		Assertions.assertArrayEquals(Arrays.copyOf(hashes(1, 2, 3), 2), a.smallestHashes());
	}

	@Test
	public void numericSlicesFoldToTheSmallestHashesOfTheWhole() {
		Random random = new Random(17);
		for (int trial = 0; trial < 100; trial++) {
			int cap = 1 + random.nextInt(60);
			long[] values = random.longs(random.nextInt(300), -100, 100).toArray();
			NumericDistinctValueCollector[] slices = { new NumericDistinctValueCollector("f", cap), new NumericDistinctValueCollector("f", cap),
					new NumericDistinctValueCollector("f", cap) };
			for (int i = 0; i < values.length; i++) {
				slices[i % 3].add(values[i]);
			}
			slices[0].merge(slices[1]);
			slices[0].merge(slices[2]);
			long[] all = Arrays.stream(values).distinct().map(DistinctCountCollectorManager::hash).sorted().toArray();
			Assertions.assertEquals(all.length > cap, slices[0].isCapped(), "trial " + trial);
			Assertions.assertEquals(Math.min(all.length, cap + 1L), slices[0].count(), "trial " + trial);
			Assertions.assertArrayEquals(Arrays.copyOf(all, Math.min(all.length, cap)), slices[0].smallestHashes(), "trial " + trial);
		}
	}

	@Test
	public void numericBatchesFoldAcrossTheirBoundary() {
		// cap 3 means a batch of 3, so 20 distinct values in descending order fill and merge several batches
		NumericDistinctValueCollector longs = new NumericDistinctValueCollector("f", 3);
		for (long value = 20; value >= 1; value--) {
			longs.add(value);
			longs.add(value);
		}
		Assertions.assertTrue(longs.isCapped());
		Assertions.assertEquals(4, longs.count());
		Assertions.assertArrayEquals(Arrays.copyOf(hashes(LongStream.rangeClosed(1, 20).toArray()), 3), longs.smallestHashes());
		Assertions.assertArrayEquals(longs.smallestHashes(), longs.smallestHashes(), "asking twice is stable");
	}

	private static long[] hashes(long... values) {
		return LongStream.of(values).map(DistinctCountCollectorManager::hash).sorted().toArray();
	}
}
