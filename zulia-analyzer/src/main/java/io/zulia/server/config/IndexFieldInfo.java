package io.zulia.server.config;

import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;

public class IndexFieldInfo {

	private final String internalFieldName;

	private final String internalSortFieldName;
	private final FieldType fieldType;
	private final String storedFieldName;
	private final ZuliaIndex.IndexAs indexAs;

	public IndexFieldInfo(String storedFieldName, String internalIndexFieldName, String internalSortFieldName, FieldType fieldType,
			ZuliaIndex.IndexAs indexAs) {
		this.storedFieldName = storedFieldName;
		this.internalFieldName = internalIndexFieldName;
		this.internalSortFieldName = internalSortFieldName;
		this.fieldType = fieldType;
		this.indexAs = indexAs;
	}

	public String getStoredFieldName() {
		return storedFieldName;
	}

	public String getInternalFieldName() {
		return internalFieldName;
	}

	public String getInternalSortFieldName() {
		return internalSortFieldName;
	}

	public FieldType getFieldType() {
		return fieldType;
	}

	public ZuliaIndex.IndexAs getIndexAs() {
		return indexAs;
	}

	@Override
	public String toString() {
		return "IndexFieldInfo{" + "internalFieldName='" + internalFieldName + '\'' + ", internalSortFieldName='" + internalSortFieldName + '\''
				+ ", fieldType=" + fieldType + ", storedFieldName='" + storedFieldName + '\'' + ", indexAs=" + indexAs + '}';
	}
}
