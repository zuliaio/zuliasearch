package io.zulia.server.search.distinct;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Distinct values of a string field as one bit per segment ordinal, per segment. Terms shared by several segments are
 * resolved when the shard payload is built, so collection needs no global ordinal map.
 */
public final class StringDistinctValueCollector implements DistinctValueCollector {

	private final String internalSortFieldName;
	private final Map<Integer, FixedBitSet> ordsByLeaf = new HashMap<>();

	public StringDistinctValueCollector(String internalSortFieldName) {
		this.internalSortFieldName = internalSortFieldName;
	}

	@Override
	public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
		SortedSetDocValues values = DocValues.getSortedSet(context.reader(), internalSortFieldName);
		return new OrdinalLeafCollector(values, forLeaf(context.ord, values.getValueCount()));
	}

	public FixedBitSet forLeaf(int leafOrd, long valueCount) {
		if (valueCount > Integer.MAX_VALUE) {
			throw new IllegalStateException(
					"Field <" + internalSortFieldName + "> has " + valueCount + " terms in one segment, more than a distinct count supports");
		}
		return ordsByLeaf.computeIfAbsent(leafOrd, _ -> new FixedBitSet((int) valueCount));
	}

	public Map<Integer, FixedBitSet> ordsByLeaf() {
		return ordsByLeaf;
	}

	@Override
	public void merge(DistinctValueCollector other) {
		for (Map.Entry<Integer, FixedBitSet> entry : ((StringDistinctValueCollector) other).ordsByLeaf.entrySet()) {
			FixedBitSet existing = ordsByLeaf.putIfAbsent(entry.getKey(), entry.getValue());
			if (existing != null) {
				existing.or(entry.getValue());
			}
		}
	}

	private record OrdinalLeafCollector(SortedSetDocValues values, FixedBitSet bits) implements LeafCollector {

		@Override
		public void setScorer(Scorable scorer) {
		}

		@Override
		public void collect(int doc) throws IOException {
			if (values.advanceExact(doc)) {
				int valueCount = values.docValueCount();
				for (int i = 0; i < valueCount; i++) {
					bits.set((int) values.nextOrd());
				}
			}
		}
	}
}
