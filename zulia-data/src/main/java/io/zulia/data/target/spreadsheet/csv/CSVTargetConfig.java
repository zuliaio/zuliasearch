package io.zulia.data.target.spreadsheet.csv;

import com.univocity.parsers.csv.CsvWriter;
import io.zulia.data.output.DataOutputStream;
import io.zulia.data.target.spreadsheet.SpreadsheetTargetConfig;
import io.zulia.data.target.spreadsheet.csv.formatter.*;

public class CSVTargetConfig extends SpreadsheetTargetConfig<CsvWriter, CSVTargetConfig> {
	
	public static CSVTargetConfig from(DataOutputStream dataStream) {
		return new CSVTargetConfig(dataStream);
	}
	
	private char delimiter = ',';
	
	public CSVTargetConfig(DataOutputStream dataStream) {
		super(dataStream);
		withStringHandler(new StringCSVWriter());
		withDateTypeHandler(new DateCSVWriter());
		withNumberTypeHandler(new NumberCSVWriter());
		withLinkTypeHandler(new LinkCSVWriter());
		withDefaultTypeHandler(new DefaultCSVWriter());
		withBooleanTypeHandler(new BooleanCSVWriter());
		withCollectionHandler(new CollectionCSVWriter(this));
		withHeaderHandler(new StringCSVWriter()); // csv headers are just strings
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
