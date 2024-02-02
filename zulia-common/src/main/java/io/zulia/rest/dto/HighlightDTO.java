package io.zulia.rest.dto;

import io.zulia.message.ZuliaQuery;

import java.util.List;

public record HighlightDTO(String field, List<String> fragments) {

	public static HighlightDTO fromHighlightResult(ZuliaQuery.HighlightResult highlightResult) {
		return new HighlightDTO(highlightResult.getField(), highlightResult.getFragmentsList());
	}
}

