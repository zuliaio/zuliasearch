package io.zulia.data.target.spreadsheet;

public interface SpreadsheetTypeHandler<T, S> {

	void writeType(T reference, S value);

}
