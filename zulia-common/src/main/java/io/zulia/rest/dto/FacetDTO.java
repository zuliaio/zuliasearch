package io.zulia.rest.dto;

import io.zulia.message.ZuliaQuery;

public record FacetDTO(String facet, long count, Long maxError) {

	public static FacetDTO fromFacetCount(ZuliaQuery.FacetCount facetCount) {
		return new FacetDTO(facetCount.getFacet(), facetCount.getCount(), facetCount.getMaxError() != 0 ? facetCount.getMaxError() : null);
	}
}
