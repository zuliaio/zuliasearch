package io.zulia.server.config;

import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;

public class SortFieldInfo {

	private final String internalSortFieldName;
	private final FieldType fieldType;

	public SortFieldInfo(String internalSortFieldName, FieldType fieldType) {

		this.internalSortFieldName = internalSortFieldName;
		this.fieldType = fieldType;
	}

	public String getInternalSortFieldName() {
		return internalSortFieldName;
	}

	public FieldType getFieldType() {
		return fieldType;
	}

	@Override
	public String toString() {
		return "SortFieldInfo{" + "internalSortFieldName='" + internalSortFieldName + '\'' + ", fieldType=" + fieldType + '}';
	}
}
