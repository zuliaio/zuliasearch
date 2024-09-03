package io.zulia.data.target.spreadsheet.csv.formatter;

import com.univocity.parsers.csv.CsvWriter;
import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;
import io.zulia.data.target.spreadsheet.excel.cell.Link;

public class LinkCSVWriter implements SpreadsheetTypeHandler<CsvWriter, Link> {
	
	@Override
	public void writeType(CsvWriter reference, Link value) {
		if (value == null) {
			reference.addValue(null);
		}
		else {
			reference.addValue(value.href());
		}
	}
}
