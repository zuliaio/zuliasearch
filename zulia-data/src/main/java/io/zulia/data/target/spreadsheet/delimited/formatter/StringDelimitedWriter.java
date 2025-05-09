package io.zulia.data.target.spreadsheet.delimited.formatter;

import com.univocity.parsers.common.AbstractWriter;
import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;

public class StringDelimitedWriter<T extends AbstractWriter<?>> implements SpreadsheetTypeHandler<T, String> {

	@Override
	public void writeType(T reference, String value) {
		reference.addValue(value);
	}
}
