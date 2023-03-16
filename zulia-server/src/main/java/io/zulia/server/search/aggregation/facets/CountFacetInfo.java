package io.zulia.server.search.aggregation.facets;

import com.koloboke.collect.map.hash.HashIntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import io.zulia.server.search.aggregation.ordinal.OrdinalConsumer;

public class CountFacetInfo extends FacetInfo implements OrdinalConsumer {

	private final HashIntIntMap countFacetInfo;

	public CountFacetInfo() {
		countFacetInfo = HashIntIntMaps.newMutableMap();
	}

	public int getOrdinalCount(int child) {
		return countFacetInfo.get(child);
	}

	@Override
	public void handleOrdinal(int ordinal) {
		countFacetInfo.addValue(ordinal, 1);
	}
}
