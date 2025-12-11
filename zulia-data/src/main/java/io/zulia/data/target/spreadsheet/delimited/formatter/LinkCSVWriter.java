package io.zulia.data.target.spreadsheet.delimited.formatter;

import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;
import io.zulia.data.target.spreadsheet.excel.cell.Link;

import java.util.List;

public class LinkCSVWriter<T extends List<String>> implements SpreadsheetTypeHandler<T, Link> {

	@Override
	public void writeType(T reference, Link value) {
		if (value == null) {
			reference.add(null);
		}
		else {
			reference.add(value.href());
		}
	}
}
