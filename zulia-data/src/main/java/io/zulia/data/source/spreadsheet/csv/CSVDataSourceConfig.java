package io.zulia.data.source.spreadsheet.csv;

import io.zulia.data.common.HeaderConfig;
import io.zulia.data.input.DataInputStream;
import io.zulia.data.source.spreadsheet.DefaultDelimitedListHandler;
import io.zulia.data.source.spreadsheet.DelimitedListHandler;

public class CSVDataSourceConfig {

	public static CSVDataSourceConfig from(DataInputStream dataStream) {
		return new CSVDataSourceConfig(dataStream);
	}

	private final DataInputStream dataInputStream;
	private char delimiter = ',';

	private HeaderConfig headerConfig;

	private DelimitedListHandler delimitedListHandler = new DefaultDelimitedListHandler(';');

	private CSVDataSourceConfig(DataInputStream dataInputStream) {
		this.dataInputStream = dataInputStream;
	}

	public CSVDataSourceConfig withDelimiter(char delimiter) {
		this.delimiter = delimiter;
		return this;
	}

	public CSVDataSourceConfig withListDelimiter(char listDelimiter) {
		this.delimitedListHandler = new DefaultDelimitedListHandler(listDelimiter);
		return this;
	}

	public CSVDataSourceConfig withDelimitedListHandler(DelimitedListHandler delimitedListHandler) {
		this.delimitedListHandler = delimitedListHandler;
		return this;
	}

	public CSVDataSourceConfig withHeaders() {
		return withHeaders(new HeaderConfig());
	}

	public CSVDataSourceConfig withHeaders(HeaderConfig headerConfig) {
		this.headerConfig = headerConfig;
		return this;
	}

	public CSVDataSourceConfig withoutHeaders() {
		this.headerConfig = null;
		return this;
	}

	public DataInputStream getDataInputStream() {
		return dataInputStream;
	}

	public char getDelimiter() {
		return delimiter;
	}

	public boolean hasHeaders() {
		return headerConfig != null;
	}

	public DelimitedListHandler getDelimitedListHandler() {
		return delimitedListHandler;
	}

	public HeaderConfig getHeaderConfig() {
		return headerConfig;
	}
}
