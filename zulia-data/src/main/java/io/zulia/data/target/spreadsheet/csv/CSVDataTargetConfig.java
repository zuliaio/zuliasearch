package io.zulia.data.target.spreadsheet.csv;

import io.zulia.data.output.DataOutputStream;
import io.zulia.data.source.spreadsheet.DefaultDelimitedListHandler;
import io.zulia.data.source.spreadsheet.DelimitedListHandler;

import java.util.Collection;

public class CSVDataTargetConfig {

	public static CSVDataTargetConfig from(DataOutputStream dataStream) {
		return new CSVDataTargetConfig(dataStream);
	}

	private final DataOutputStream dataStream;

	private char delimiter = ',';

	private Collection<String> headers;

	private DelimitedListHandler delimitedListHandler = new DefaultDelimitedListHandler(';');

	private CSVDataTargetConfig(DataOutputStream dataStream) {
		this.dataStream = dataStream;
	}

	public DataOutputStream getDataStream() {
		return dataStream;
	}

	public CSVDataTargetConfig withDelimiter(char delimiter) {
		this.delimiter = delimiter;
		return this;
	}

	public CSVDataTargetConfig withListDelimiter(char listDelimiter) {
		this.delimitedListHandler = new DefaultDelimitedListHandler(listDelimiter);
		return this;
	}

	public CSVDataTargetConfig withDelimitedListHandler(DelimitedListHandler delimitedListHandler) {
		this.delimitedListHandler = delimitedListHandler;
		return this;
	}

	public CSVDataTargetConfig withHeader(Collection<String> headers) {
		this.headers = headers;
		return this;
	}

	public Collection<String> getHeaders() {
		return headers;
	}

	public char getDelimiter() {
		return delimiter;
	}

	public DelimitedListHandler getDelimitedListHandler() {
		return delimitedListHandler;
	}

}
