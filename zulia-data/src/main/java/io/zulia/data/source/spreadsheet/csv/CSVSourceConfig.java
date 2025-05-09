package io.zulia.data.source.spreadsheet.csv;

import io.zulia.data.input.DataInputStream;
import io.zulia.data.source.spreadsheet.delimited.DelimitedSourceConfig;

public class CSVSourceConfig extends DelimitedSourceConfig {

	public CSVSourceConfig(DataInputStream dataInputStream) {
		super(dataInputStream);
	}

	public static CSVSourceConfig from(DataInputStream dataStream) {
		return new CSVSourceConfig(dataStream);
	}

	private char delimiter = ',';

	public CSVSourceConfig withDelimiter(char delimiter) {
		this.delimiter = delimiter;
		return this;
	}

	public char getDelimiter() {
		return delimiter;
	}
}
