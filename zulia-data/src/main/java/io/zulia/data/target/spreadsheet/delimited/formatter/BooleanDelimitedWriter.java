package io.zulia.data.target.spreadsheet.delimited.formatter;

import com.univocity.parsers.common.AbstractWriter;
import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;

public class BooleanDelimitedWriter<T extends AbstractWriter<?>> implements SpreadsheetTypeHandler<T, Boolean> {
	
	@Override
	public void writeType(T reference, Boolean value) {
		if (value != null) {
			reference.addValue(value ? "True" : "False");
		}
		else {
			reference.addValue(null);
		}
	}
}
