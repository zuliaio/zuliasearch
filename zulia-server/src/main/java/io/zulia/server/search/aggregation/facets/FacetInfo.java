package io.zulia.server.search.aggregation.facets;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class FacetInfo {
	private Set<String> facets;

	private Set<Integer> dimensionOrdinals;

	private int[] dimensionOrdinalsArray;

	public FacetInfo() {
		this.facets = new HashSet<>();
		this.dimensionOrdinals = new HashSet<>();
	}

	public void addFacet(String facet, int dimensionOrdinal) {
		facets.add(facet);
		dimensionOrdinals.add(dimensionOrdinal);
	}

	public boolean hasFacets() {
		return !facets.isEmpty();
	}

	public void computeSortedOrdinalArray() {
		dimensionOrdinalsArray = dimensionOrdinals.stream().mapToInt(Integer::intValue).toArray();
		Arrays.sort(dimensionOrdinalsArray);
	}

	public int[] requestedDimensionOrdinals() {
		return dimensionOrdinalsArray;
	}

}
