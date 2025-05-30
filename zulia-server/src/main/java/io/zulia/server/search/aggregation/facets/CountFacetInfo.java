package io.zulia.server.search.aggregation.facets;

import io.zulia.server.search.aggregation.ordinal.OrdinalConsumer;

public class CountFacetInfo extends FacetInfo implements OrdinalConsumer {

	private final SegmentedIntIntMap countFacetInfo;

	public CountFacetInfo() {
		countFacetInfo = new SegmentedIntIntMap(32);
	}

	public int getOrdinalCount(int child) {
		return countFacetInfo.get(child);
	}

	@Override
	public void handleOrdinal(int ordinal) {
		countFacetInfo.increment(ordinal);
	}
}
