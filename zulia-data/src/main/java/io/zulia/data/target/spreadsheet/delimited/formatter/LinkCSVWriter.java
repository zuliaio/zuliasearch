package io.zulia.data.target.spreadsheet.delimited.formatter;

import com.univocity.parsers.common.AbstractWriter;
import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;
import io.zulia.data.target.spreadsheet.excel.cell.Link;

public class LinkCSVWriter<T extends AbstractWriter<?>> implements SpreadsheetTypeHandler<T, Link> {

	@Override
	public void writeType(T reference, Link value) {
		if (value == null) {
			reference.addValue(null);
		}
		else {
			reference.addValue(value.href());
		}
	}
}
