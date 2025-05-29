package io.zulia.data.target.spreadsheet.delimited.formatter;

import com.univocity.parsers.common.AbstractWriter;
import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;

public class DefaultCSVWriter<T extends AbstractWriter<?>> implements SpreadsheetTypeHandler<T, Object> {
	
	@Override
	public void writeType(T reference, Object value) {
		reference.addValue(value);
	}
}
