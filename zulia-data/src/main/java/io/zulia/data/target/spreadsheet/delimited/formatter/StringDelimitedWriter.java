package io.zulia.data.target.spreadsheet.delimited.formatter;

import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;

import java.util.List;

public class StringDelimitedWriter<T extends List<String>> implements SpreadsheetTypeHandler<T, String> {

	@Override
	public void writeType(T reference, String value) {
		reference.add(value);
	}
}
