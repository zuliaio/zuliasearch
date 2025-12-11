package io.zulia.data.target.spreadsheet.delimited.formatter;

import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;

import java.util.List;

public class NumberCSVWriter<T extends List<String>> implements SpreadsheetTypeHandler<T, Number> {

	private int decimalPlaces;
	private String doubleFormatter;

	public NumberCSVWriter() {
		this(3);
	}

	public NumberCSVWriter(int decimalPlaces) {
		withDecimalPlaces(decimalPlaces);
	}

	public NumberCSVWriter<T> withDecimalPlaces(int decimalPlaces) {
		this.doubleFormatter = "%." + decimalPlaces + "f";
		this.decimalPlaces = decimalPlaces;
		return this;
	}

	public int getDecimalPlaces() {
		return decimalPlaces;
	}

	@Override
	public void writeType(T reference, Number value) {
		switch (value) {
			case Integer i -> reference.add(String.valueOf(i));
			case Long l -> reference.add(String.valueOf(l));
			case Float f -> reference.add(String.format(doubleFormatter, f));
			case Double d -> reference.add(String.format(doubleFormatter, d));
			case null, default -> reference.add(null);
		}
	}
}
