package io.zulia.server.config;

import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;

public class SortFieldInfo {

	private final String internalSortFieldName;
	private final FieldType fieldType;
	private final ZuliaIndex.SortAs.StringHandling stringHandling;

	public SortFieldInfo(String internalSortFieldName, FieldType fieldType, ZuliaIndex.SortAs.StringHandling stringHandling) {

		this.internalSortFieldName = internalSortFieldName;
		this.fieldType = fieldType;
		this.stringHandling = stringHandling;
	}

	public String getInternalSortFieldName() {
		return internalSortFieldName;
	}

	public FieldType getFieldType() {
		return fieldType;
	}

	public ZuliaIndex.SortAs.StringHandling getStringHandling() {
		return stringHandling;
	}

	@Override
	public String toString() {
		return "SortFieldInfo{" + "internalSortFieldName='" + internalSortFieldName + '\'' + ", fieldType=" + fieldType + ", stringHandling=" + stringHandling
				+ '}';
	}
}
