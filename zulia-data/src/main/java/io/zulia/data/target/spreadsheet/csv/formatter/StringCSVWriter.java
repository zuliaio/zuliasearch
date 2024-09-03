package io.zulia.data.target.spreadsheet.csv.formatter;

import com.univocity.parsers.csv.CsvWriter;
import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;

public class StringCSVWriter implements SpreadsheetTypeHandler<CsvWriter, String> {
	
	@Override
	public void writeType(CsvWriter reference, String value) {
		reference.addValue(value);
	}
}
