package io.zulia.data.source.spreadsheet;

import io.zulia.data.source.DataSource;

import java.io.IOException;
import java.util.SequencedSet;

public interface SpreadsheetSource<T extends SpreadsheetRecord> extends DataSource<T> {

	boolean hasHeader(String field);

	SequencedSet<String> getHeaders();

	void close() throws IOException;
}
