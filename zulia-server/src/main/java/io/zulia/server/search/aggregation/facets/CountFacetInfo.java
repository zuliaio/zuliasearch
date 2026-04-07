package io.zulia.server.search.aggregation.facets;

import io.zulia.server.search.aggregation.ordinal.FacetHandler;
import io.zulia.server.search.aggregation.ordinal.OrdinalConsumer;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;

public class CountFacetInfo extends FacetInfo implements OrdinalConsumer {

	private final IntIntHashMap countFacetInfo;

	public CountFacetInfo() {
		countFacetInfo = new IntIntHashMap();
	}

	private CountFacetInfo(CountFacetInfo copyFacetInfo) {
		super(copyFacetInfo);
		countFacetInfo = new IntIntHashMap();
	}

	public CountFacetInfo cloneNewCounter() {
		return new CountFacetInfo(this);
	}

	public int getOrdinalCount(int child) {
		return countFacetInfo.get(child);
	}

	@Override
	public void handleOrdinal(int ordinal) {
		countFacetInfo.addToValue(ordinal, 1);
	}

	public synchronized void merge(CountFacetInfo other) {
		other.countFacetInfo.forEachKeyValue((key, value) -> countFacetInfo.addToValue(key, value));
	}

	public void maybeHandleFacets(FacetHandler facetHandler) {
		if (hasFacets()) {
			facetHandler.handleFacets(this);
		}
	}
}
