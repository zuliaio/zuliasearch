package io.zulia.server.search.aggregation.facets;

import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import io.zulia.server.search.aggregation.ordinal.FacetHandler;
import io.zulia.server.search.aggregation.ordinal.OrdinalConsumer;

import java.util.Map;

public class CountFacetInfo extends FacetInfo implements OrdinalConsumer {

	private final IntIntMap countFacetInfo;


	public CountFacetInfo() {
		countFacetInfo = HashIntIntMaps.newMutableMap();
	}

	private CountFacetInfo(CountFacetInfo copyFacetInfo) {
		super(copyFacetInfo);
		countFacetInfo = HashIntIntMaps.newMutableMap();
	}

	public CountFacetInfo cloneNewCounter() {
		return new CountFacetInfo(this);
	}

	public int getOrdinalCount(int child) {
		return countFacetInfo.get(child);
	}

	@Override
	public void handleOrdinal(int ordinal) {
		countFacetInfo.addValue(ordinal, 1);
	}

	public synchronized void merge(CountFacetInfo other) {
		for (Map.Entry<Integer, Integer> localKeyToValue : other.countFacetInfo.entrySet()) {
			countFacetInfo.addValue(localKeyToValue.getKey(), localKeyToValue.getValue());
		}
	}

	public void maybeHandleFacets(FacetHandler facetHandler) {
		if (hasFacets()) {
			facetHandler.handleFacets(this);
		}
	}
}
