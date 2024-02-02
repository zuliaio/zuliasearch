package io.zulia.rest.dto;

import io.zulia.message.ZuliaQuery;

import java.util.List;

public record FacetsDTO(String field, Long maxPossibleMissing, List<FacetDTO> values) {
	public static FacetsDTO fromFacetGroup(ZuliaQuery.FacetGroup facetGroup) {
		return new FacetsDTO(facetGroup.getCountRequest().getFacetField().getLabel(), facetGroup.getMaxValuePossibleMissing(),
				facetGroup.getFacetCountList().stream().map(FacetDTO::fromFacetCount).toList());
	}
}
