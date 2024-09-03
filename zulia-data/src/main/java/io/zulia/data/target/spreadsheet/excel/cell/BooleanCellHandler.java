package io.zulia.data.target.spreadsheet.excel.cell;

import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;

public class BooleanCellHandler implements SpreadsheetTypeHandler<CellReference, Boolean> {
	
	@Override
	public void writeType(CellReference reference, Boolean value) {
		if (value != null) {
			reference.cell().setCellValue(value);
		}
		else {
			reference.cell().setBlank();
		}
	}
}