package io.zulia.data.target.spreadsheet.csv.formatter;

import com.univocity.parsers.csv.CsvWriter;
import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;

public class NumberCSVWriter implements SpreadsheetTypeHandler<CsvWriter, Number> {
	
	private int decimalPlaces;
	private String doubleFormatter;
	
	public NumberCSVWriter() {
		this(3);
	}
	
	public NumberCSVWriter(int decimalPlaces) {
		withDecimalPlaces(decimalPlaces);
	}
	
	public NumberCSVWriter withDecimalPlaces(int decimalPlaces) {
		this.doubleFormatter = "%." + decimalPlaces + "f";
		this.decimalPlaces = decimalPlaces;
		return this;
	}
	
	public int getDecimalPlaces() {
		return decimalPlaces;
	}
	
	@Override
	public void writeType(CsvWriter reference, Number value) {
		switch (value) {
			case Integer i -> reference.addValue(String.valueOf(i));
			case Long l -> reference.addValue(String.valueOf(l));
			case Float f -> reference.addValue(String.format(doubleFormatter, f));
			case Double d -> reference.addValue(String.format(doubleFormatter, d));
			case null, default -> reference.addValue(null);
		}
	}
}
