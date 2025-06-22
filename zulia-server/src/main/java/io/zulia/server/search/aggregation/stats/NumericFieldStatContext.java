package io.zulia.server.search.aggregation.stats;

import io.zulia.server.search.aggregation.ordinal.FacetHandler;
import io.zulia.server.search.aggregation.ordinal.OrdinalConsumer;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;

import java.io.IOException;

public class NumericFieldStatContext implements OrdinalConsumer {

	private final SortedNumericDocValues numericDocValues;
	private final NumericFieldStatInfo localNumericFieldStatInfo;
	private final NumericFieldStatInfo globalNumericFieldStatInfo;

	private long[] numericValues;
	private int numericValueCount;

	public NumericFieldStatContext(LeafReader reader, NumericFieldStatInfo globalNumericFieldStatInfo) throws IOException {
		this.globalNumericFieldStatInfo = globalNumericFieldStatInfo;
		this.localNumericFieldStatInfo = globalNumericFieldStatInfo.cloneNewStatCount();
		this.numericDocValues = DocValues.getSortedNumeric(reader, localNumericFieldStatInfo.getSortFieldName());
		this.numericValues = new long[1];
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

	@Override
	public void handleOrdinal(int ordinal) {
		Stats<?> stats = localNumericFieldStatInfo.getFacetStatStorage().getOrCreateStat(ordinal);
		stats.handleNumericValues(numericValues, numericValueCount);
	}

	@Override
	public int[] requestedDimensionOrdinals() {
		return localNumericFieldStatInfo.requestedDimensionOrdinals();
	}

	public void maybeHandleFacet(FacetHandler facetHandler) {
		if (localNumericFieldStatInfo.hasFacets()) {
			facetHandler.handleFacets(this);
		}
	}

	public void maybeHandleGlobal() {
		if (localNumericFieldStatInfo.hasGlobal()) {
			Stats<?> stats = localNumericFieldStatInfo.getGlobalStats();
			stats.handleNumericValues(numericValues, numericValueCount);
		}
	}

	public void mergeStats() {
		globalNumericFieldStatInfo.merge(localNumericFieldStatInfo);
	}
}
