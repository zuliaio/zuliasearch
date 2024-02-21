package io.zulia.data.target.spreadsheet.excel;

import io.zulia.data.output.DataOutputStream;
import io.zulia.data.source.spreadsheet.DefaultDelimitedListHandler;
import io.zulia.data.source.spreadsheet.DelimitedListHandler;

public class ExcelDataTargetConfig {

	public static ExcelDataTargetConfig from(DataOutputStream dataStream) {
		return new ExcelDataTargetConfig(dataStream);
	}

	private final DataOutputStream dataStream;

	private char delimiter = ',';

	private DelimitedListHandler delimitedListHandler = new DefaultDelimitedListHandler(';');

	private ExcelDataTargetConfig(DataOutputStream dataStream) {
		this.dataStream = dataStream;
	}

	public DataOutputStream getDataStream() {
		return dataStream;
	}

	public ExcelDataTargetConfig withDelimiter(char delimiter) {
		this.delimiter = delimiter;
		return this;
	}

	public ExcelDataTargetConfig withListDelimiter(char listDelimiter) {
		this.delimitedListHandler = new DefaultDelimitedListHandler(listDelimiter);
		return this;
	}

	public ExcelDataTargetConfig withDelimitedListHandler(DelimitedListHandler delimitedListHandler) {
		this.delimitedListHandler = delimitedListHandler;
		return this;
	}

	public char getDelimiter() {
		return delimiter;
	}

	public DelimitedListHandler getDelimitedListHandler() {
		return delimitedListHandler;
	}

}
