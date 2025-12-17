package io.zulia.data.target.spreadsheet.delimited.formatter;

import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;

import java.util.List;

public class BooleanDelimitedWriter<T extends List<String>> implements SpreadsheetTypeHandler<T, Boolean> {

	@Override
	public void writeType(T reference, Boolean value) {
		if (value != null) {
			reference.add(value ? "True" : "False");
		}
		else {
			reference.add(null);
		}
	}
}
