package io.zulia.data.target.spreadsheet.csv;

import com.univocity.parsers.csv.CsvWriter;
import io.zulia.data.output.DataOutputStream;
import io.zulia.data.target.spreadsheet.SpreadsheetDataTargetConfig;
import io.zulia.data.target.spreadsheet.csv.formatter.*;

public class CSVDataTargetConfig extends SpreadsheetDataTargetConfig<CsvWriter, CSVDataTargetConfig> {
	
	public static CSVDataTargetConfig from(DataOutputStream dataStream) {
		return new CSVDataTargetConfig(dataStream);
	}
	
	private char delimiter = ',';
	
	public CSVDataTargetConfig(DataOutputStream dataStream) {
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
	protected CSVDataTargetConfig getSelf() {
		return this;
	}
	
	public CSVDataTargetConfig withDelimiter(char delimiter) {
		this.delimiter = delimiter;
		return this;
	}
	
	public char getDelimiter() {
		return delimiter;
	}
	
}
