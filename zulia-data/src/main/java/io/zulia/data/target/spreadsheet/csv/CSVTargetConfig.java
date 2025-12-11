package io.zulia.data.target.spreadsheet.csv;

import io.zulia.data.output.DataOutputStream;
import io.zulia.data.target.spreadsheet.delimited.DelimitedTargetConfig;

import java.util.List;

public class CSVTargetConfig extends DelimitedTargetConfig<List<String>, CSVTargetConfig> {

	public static CSVTargetConfig from(DataOutputStream dataStream) {
		return new CSVTargetConfig(dataStream);
	}

	private char delimiter = ',';

	public CSVTargetConfig(DataOutputStream dataStream) {
		super(dataStream);

	}

	@Override
	protected CSVTargetConfig getSelf() {
		return this;
	}

	public CSVTargetConfig withDelimiter(char delimiter) {
		this.delimiter = delimiter;
		return this;
	}

	public char getDelimiter() {
		return delimiter;
	}

}
