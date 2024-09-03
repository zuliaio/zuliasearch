package io.zulia.data.target.spreadsheet.csv.formatter;

import com.univocity.parsers.csv.CsvWriter;
import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;

public class DefaultCSVWriter implements SpreadsheetTypeHandler<CsvWriter, Object> {
	
	@Override
	public void writeType(CsvWriter reference, Object value) {
		if (value != null) {
			reference.addValue(value);
		}
		else {
			reference.addValue(null);
		}
	}
}
