package io.zulia.data.target.spreadsheet.delimited.formatter;

import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;

import java.util.List;

public class DefaultCSVWriter<T extends List<String>> implements SpreadsheetTypeHandler<T, Object> {

	@Override
	public void writeType(T reference, Object value) {
		reference.add(value != null ? value.toString() : null);
	}
}
