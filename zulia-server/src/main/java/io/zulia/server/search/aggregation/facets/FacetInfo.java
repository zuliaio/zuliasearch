package io.zulia.server.search.aggregation.facets;

import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.ObjSet;
import com.koloboke.collect.set.hash.HashIntSets;
import com.koloboke.collect.set.hash.HashObjSets;

import java.util.Arrays;

public class FacetInfo {
	private ObjSet<String> facets;

	private IntSet dimensionOrdinals;

	private int[] dimensionOrdinalsArray;

	public FacetInfo() {
		this.facets = HashObjSets.newMutableSet();
		this.dimensionOrdinals = HashIntSets.newMutableSet();
	}

	public void addFacet(String facet, int dimensionOrdinal) {
		facets.add(facet);
		dimensionOrdinals.add(dimensionOrdinal);
	}

	public boolean hasFacets() {
		return !facets.isEmpty();
	}

	public void computeSortedOrdinalArray() {
		dimensionOrdinalsArray = dimensionOrdinals.toIntArray();
		Arrays.sort(dimensionOrdinalsArray);
	}

	public int[] requestedDimensionOrdinals() {
		return dimensionOrdinalsArray;
	}

}
