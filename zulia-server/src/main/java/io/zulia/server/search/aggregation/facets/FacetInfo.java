package io.zulia.server.search.aggregation.facets;

import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.ObjSet;
import com.koloboke.collect.set.hash.HashIntSets;
import com.koloboke.collect.set.hash.HashObjSets;

import java.util.Arrays;

public class FacetInfo {
	private final ObjSet<String> facets;
	private final IntSet dimensionOrdinals;
	private int[] dimensionOrdinalsArray;
	private boolean hasFacets = false;

	public FacetInfo() {
		this.facets = HashObjSets.newMutableSet();
		this.dimensionOrdinals = HashIntSets.newMutableSet();
	}

	public FacetInfo(FacetInfo facetInfo) {
		this.facets = facetInfo.facets;
		this.dimensionOrdinals = facetInfo.dimensionOrdinals;
		this.dimensionOrdinalsArray = facetInfo.dimensionOrdinalsArray;
		this.hasFacets = facetInfo.hasFacets;
	}

	public void addFacet(String facet, int dimensionOrdinal) {
		facets.add(facet);
		dimensionOrdinals.add(dimensionOrdinal);
		hasFacets = true;
	}

	public boolean hasFacets() {
		return hasFacets;
	}

	public void computeSortedOrdinalArray() {
		dimensionOrdinalsArray = dimensionOrdinals.toIntArray();
		Arrays.sort(dimensionOrdinalsArray);
	}

	public int[] requestedDimensionOrdinals() {
		return dimensionOrdinalsArray;
	}

}
