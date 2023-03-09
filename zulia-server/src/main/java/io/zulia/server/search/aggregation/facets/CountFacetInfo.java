package io.zulia.server.search.aggregation.facets;

import com.koloboke.collect.map.hash.HashIntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;

public class CountFacetInfo extends FacetInfo {

	private final HashIntIntMap countFacetInfo;

	public CountFacetInfo() {
		countFacetInfo = HashIntIntMaps.newMutableMap();
	}

	public void tallyOrdinal(int ordinal) {
		countFacetInfo.addValue(ordinal, 1);
	}

	public int getOrdinalCount(int child) {
		return countFacetInfo.get(child);
	}
}
