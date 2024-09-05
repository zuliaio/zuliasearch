package io.zulia.data.target.spreadsheet.csv.formatter;

import com.univocity.parsers.csv.CsvWriter;
import io.zulia.data.source.spreadsheet.DelimitedListHandler;
import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;
import io.zulia.data.target.spreadsheet.csv.CSVTargetConfig;

import java.util.Collection;

public class CollectionCSVWriter implements SpreadsheetTypeHandler<CsvWriter, Collection<?>> {
	
	private final CSVTargetConfig csvDataTargetConfig;
	
	public CollectionCSVWriter(CSVTargetConfig csvDataTargetConfig) {
		this.csvDataTargetConfig = csvDataTargetConfig;
	}
	
	@Override
	public void writeType(CsvWriter reference, Collection<?> value) {
		if (value != null) {
			DelimitedListHandler delimitedListHandler = csvDataTargetConfig.getDelimitedListHandler();
			String s = delimitedListHandler.collectionToCellValue(value);
			SpreadsheetTypeHandler<CsvWriter, String> stringCellHandler = csvDataTargetConfig.getStringTypeHandler();
			stringCellHandler.writeType(reference, s);
		}
		else {
			reference.addValue(null);
		}
	}
}
