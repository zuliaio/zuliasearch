package io.zulia.data.source.spreadsheet.tsv;

import io.zulia.data.common.HeaderMapping;
import io.zulia.data.source.spreadsheet.delimited.DelimitedRecord;

public class TSVRecord extends DelimitedRecord {

	public TSVRecord(String[] row, HeaderMapping headerMapping, TSVSourceConfig tsvSourceConfig) {
		super(row, headerMapping, tsvSourceConfig);
	}

}
