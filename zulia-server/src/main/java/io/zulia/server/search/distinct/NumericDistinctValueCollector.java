package io.zulia.server.search.distinct;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import java.io.IOException;
import java.util.Arrays;

/**
 * Distinct values of a numeric, date or boolean field, all stored as long doc values. Under the cap every distinct value
 * is held as is. Past it the shard keeps only the cap smallest of the values' hashes, which spread evenly over the long
 * range, so they are a sketch of the count as well as a floor of it. The hash is one to one, so it never merges two
 * values, and past the cap the count is a floor of cap + 1.
 */
public final class NumericDistinctValueCollector implements DistinctValueCollector {

	private final String internalSortFieldName;
	private final int cap;
	// the raw values until the cap, dropped once hashing starts
	private MutableLongSet values = new LongHashSet();
	// past the cap: the cap smallest hashes in ascending order, and a batch of hashes below the largest kept waiting to be merged in
	private long[] kept;
	private long[] batch;
	private int batched;

	public NumericDistinctValueCollector(String internalSortFieldName, int cap) {
		this.internalSortFieldName = internalSortFieldName;
		this.cap = cap;
	}

	@Override
	public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
		return new LongLeafCollector(DocValues.getSortedNumeric(context.reader(), internalSortFieldName), this);
	}

	public void add(long value) {
		if (kept == null) {
			if (values.size() < cap) {
				values.add(value);
				return;
			}
			if (values.contains(value)) {
				return;
			}
			// one past the cap, so from here on only the smallest hashes are kept
			startKeepingSmallest();
		}
		addHash(DistinctCountCollectorManager.hash(value));
	}

	private void startKeepingSmallest() {
		kept = values.toArray();
		for (int i = 0; i < kept.length; i++) {
			kept[i] = DistinctCountCollectorManager.hash(kept[i]);
		}
		Arrays.sort(kept);
		values = null;
		batch = new long[cap];
	}

	private void addHash(long hash) {
		if (hash >= kept[kept.length - 1]) {
			return;
		}
		batch[batched++] = hash;
		if (batched == batch.length) {
			mergeBatch();
		}
	}

	/**
	 * Folds the batch into the kept hashes, keeping the cap smallest of both. Sorting first lets one merge do the work.
	 */
	private void mergeBatch() {
		if (batched == 0) {
			return;
		}
		Arrays.sort(batch, 0, batched);
		int distinct = 0;
		for (int i = 0; i < batched; i++) {
			if (i == 0 || batch[i] != batch[i - 1]) {
				batch[distinct++] = batch[i];
			}
		}
		kept = SortedLongUnion.smallestUnion(SortedLongUnion.Run.of(kept), new SortedLongUnion.Run(batch, 0, distinct), cap);
		batched = 0;
	}

	/**
	 * The distinct count, or one past the cap once the shard stopped holding every value.
	 */
	public long count() {
		return kept != null ? cap + 1L : values.size();
	}

	public boolean isCapped() {
		return kept != null;
	}

	/**
	 * The cap smallest hashes seen, in ascending signed order.
	 */
	public long[] smallestHashes() {
		if (kept != null) {
			mergeBatch();
			return kept;
		}
		long[] hashes = values.toArray();
		for (int i = 0; i < hashes.length; i++) {
			hashes[i] = DistinctCountCollectorManager.hash(hashes[i]);
		}
		Arrays.sort(hashes);
		return hashes;
	}

	@Override
	public void merge(DistinctValueCollector other) {
		NumericDistinctValueCollector o = (NumericDistinctValueCollector) other;
		if (o.kept == null) {
			o.values.forEach(this::add);
		}
		else if (kept == null) {
			// the other side already keeps the smallest, so it absorbs this side's values and this side takes over its state
			values.forEach(o::add);
			values = null;
			kept = o.kept;
			batch = o.batch;
			batched = o.batched;
		}
		else {
			// the smallest of a union are among each side's smallest, so the other side's kept hashes are all it has to offer
			o.mergeBatch();
			for (long hash : o.kept) {
				addHash(hash);
			}
		}
	}

	private record LongLeafCollector(SortedNumericDocValues values, NumericDistinctValueCollector target) implements LeafCollector {

		@Override
		public void setScorer(Scorable scorer) {
		}

		@Override
		public void collect(int doc) throws IOException {
			if (values.advanceExact(doc)) {
				int valueCount = values.docValueCount();
				for (int i = 0; i < valueCount; i++) {
					target.add(values.nextValue());
				}
			}
		}
	}
}
