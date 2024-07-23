package io.zulia.data.source.spreadsheet;

import io.zulia.data.source.DataSource;

import java.util.SequencedSet;

public interface SpreadsheetDataSource<T extends SpreadsheetRecord> extends DataSource<T> {

	boolean hasHeader(String field);

	SequencedSet<String> getHeaders();
}
