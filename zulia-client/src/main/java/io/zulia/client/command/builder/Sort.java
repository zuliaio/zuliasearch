package io.zulia.client.command.builder;

import io.zulia.message.ZuliaQuery.FieldSort;
import io.zulia.message.ZuliaQuery.FieldSort.Direction;

public class Sort implements SortBuilder {

	private final FieldSort.Builder sortBuilder;

	public Sort(String field) {
		sortBuilder = FieldSort.newBuilder().setSortField(field).setDirection(Direction.ASCENDING);
	}

	public Sort descending() {
		sortBuilder.setDirection(Direction.DESCENDING);
		return this;
	}

	public Sort ascending() {
		sortBuilder.setDirection(Direction.ASCENDING);
		return this;
	}

	public Sort missingLast() {
		sortBuilder.setMissingLast(true);
		return this;
	}

	public Sort missingFirst() {
		sortBuilder.setMissingLast(false);
		return this;
	}

	@Override
	public FieldSort getSort() {
		return sortBuilder.build();
	}
}
