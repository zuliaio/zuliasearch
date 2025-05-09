package io.zulia.data.target.spreadsheet.csv;

import com.univocity.parsers.csv.CsvWriter;
import io.zulia.data.output.DataOutputStream;
import io.zulia.data.target.spreadsheet.delimited.DelimitedTargetConfig;

public class CSVTargetConfig extends DelimitedTargetConfig<CsvWriter, CSVTargetConfig> {

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
