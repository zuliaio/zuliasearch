package io.zulia.server.search.aggregation.stats;

import io.zulia.server.search.aggregation.ordinal.OrdinalConsumer;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;

import java.io.IOException;

public class NumericFieldStatContext implements OrdinalConsumer {

	private final SortedNumericDocValues numericDocValues;

	private final NumericFieldStatInfo numericFieldStatInfo;

	private long[] numericValues;
	private int numericValueCount;

	public NumericFieldStatContext(LeafReader reader, NumericFieldStatInfo numericFieldStatInfo) throws IOException {
		this.numericFieldStatInfo = numericFieldStatInfo;
		this.numericDocValues = DocValues.getSortedNumeric(reader, numericFieldStatInfo.getSortFieldName());
		this.numericValues = new long[1];
	}

	public Stats<?> getGlobalStats() {
		return numericFieldStatInfo.getGlobalStats();
	}

	public boolean hasGlobal() {
		return numericFieldStatInfo.hasGlobal();
	}

	public boolean hasFacets() {
		return numericFieldStatInfo.hasFacets();
	}

	public void advanceNumericValues(int doc) throws IOException {
		numericValueCount = -1;
		if (numericDocValues.advanceExact(doc)) {
			numericValueCount = numericDocValues.docValueCount();
			if (numericValueCount > numericValues.length) {
				numericValues = new long[numericValueCount];
			}
			for (int j = 0; j < numericValueCount; j++) {
				numericValues[j] = numericDocValues.nextValue();
			}
		}
	}

	public long[] getNumericValues() {
		return numericValues;
	}

	public int getNumericValueCount() {
		return numericValueCount;
	}

	@Override
	public void handleOrdinal(int ordinal) {
		Stats<?> stats = numericFieldStatInfo.getFacetStatStorage().getOrCreateStat(ordinal);
		stats.handleNumericValues(numericValues, numericValueCount);
	}

	@Override
	public int[] requestedDimensionOrdinals() {
		return numericFieldStatInfo.requestedDimensionOrdinals();
	}

}