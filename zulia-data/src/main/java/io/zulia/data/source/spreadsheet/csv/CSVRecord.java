package io.zulia.data.source.spreadsheet.csv;

import io.zulia.data.common.HeaderMapping;
import io.zulia.data.source.spreadsheet.delimited.DelimitedRecord;

public class CSVRecord extends DelimitedRecord {
	
	public CSVRecord(String[] row, HeaderMapping headerMapping, CSVSourceConfig csvSourceConfig) {
		super(row, headerMapping, csvSourceConfig);
	}
	
}
