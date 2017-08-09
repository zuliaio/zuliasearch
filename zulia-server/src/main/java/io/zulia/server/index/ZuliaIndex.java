package io.zulia.server.index;

import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;

public class ZuliaIndex {

	public int getNumberOfShards() {
		return 0;
	}

	public FieldType getSortFieldType(String sortField) {
		return null;
	}

	public double getShardTolerance() {
		return 0;
	}
}
