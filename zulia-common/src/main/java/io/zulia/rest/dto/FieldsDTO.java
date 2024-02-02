package io.zulia.rest.dto;

import java.util.List;

public class FieldsDTO {
	private String index;
	private List<String> fields;

	public FieldsDTO() {
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public List<String> getFields() {
		return fields;
	}

	public void setFields(List<String> fields) {
		this.fields = fields;
	}

	@Override
	public String toString() {
		return "FieldsDTO{" + "index='" + index + '\'' + ", fields=" + fields + '}';
	}
}
