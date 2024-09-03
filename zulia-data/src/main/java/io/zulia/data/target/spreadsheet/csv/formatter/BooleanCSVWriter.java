package io.zulia.data.target.spreadsheet.csv.formatter;

import com.univocity.parsers.csv.CsvWriter;
import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;

public class BooleanCSVWriter implements SpreadsheetTypeHandler<CsvWriter, Boolean> {
	
	@Override
	public void writeType(CsvWriter reference, Boolean value) {
		if (value != null) {
			reference.addValue(value ? "True" : "False");
		}
		else {
			reference.addValue(null);
		}
	}
}
