package io.zulia.server.search.aggregation.facets;

import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.util.Arrays;
import java.util.Set;

public class FacetInfo {
	private final Set<String> facets;
	private final IntHashSet dimensionOrdinals;
	private int[] dimensionOrdinalsArray;
	private boolean hasFacets = false;

	public FacetInfo() {
		this.facets = new UnifiedSet<>();
		this.dimensionOrdinals = new IntHashSet();
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
		dimensionOrdinalsArray = dimensionOrdinals.toArray();
		Arrays.sort(dimensionOrdinalsArray);
	}

	public int[] requestedDimensionOrdinals() {
		return dimensionOrdinalsArray;
	}

}
