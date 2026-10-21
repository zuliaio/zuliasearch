package io.zulia.rest.dto;

import io.zulia.message.ZuliaQuery;

public record DistinctCountDTO(String field, long count, boolean exact) {
	public static DistinctCountDTO fromDistinctCount(ZuliaQuery.DistinctCount distinctCount) {
		return new DistinctCountDTO(distinctCount.getField(), distinctCount.getCount(), distinctCount.getExact());
	}
}
