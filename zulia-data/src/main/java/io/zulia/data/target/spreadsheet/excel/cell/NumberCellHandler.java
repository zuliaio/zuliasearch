package io.zulia.data.target.spreadsheet.excel.cell;

import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;
import org.apache.poi.ss.usermodel.BuiltinFormats;
import org.apache.poi.ss.usermodel.CellStyle;

public class NumberCellHandler implements SpreadsheetTypeHandler<CellReference, Number> {

	@Override
	public void writeType(CellReference reference, Number value) {
		if (value != null) {
			if (value instanceof Float || value instanceof Double) {
				CellStyle floatingPointStyle = reference.workbookHelper().createOrGetStyle("floatingPointStyle", this::configureFloatingPointStyle);
				reference.cell().setCellStyle(floatingPointStyle);
			}
			reference.cell().setCellValue(value.doubleValue());
		}
		else {
			reference.cell().setBlank();
		}
	}

	public void configureFloatingPointStyle(CellStyle style) {
		style.setDataFormat((short) BuiltinFormats.getBuiltinFormat("0.00"));
	}
}