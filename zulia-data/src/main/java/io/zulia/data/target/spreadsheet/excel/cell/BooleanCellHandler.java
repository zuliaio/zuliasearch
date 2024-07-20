package io.zulia.data.target.spreadsheet.excel.cell;

import io.zulia.data.target.spreadsheet.excel.WorkbookHelper;
import org.apache.poi.xssf.streaming.SXSSFCell;

public class BooleanCellHandler implements CellHandler<Boolean> {

	@Override
	public void handleCell(WorkbookHelper workbookHelper, SXSSFCell cell, Boolean bool) {
		if (bool != null) {
			cell.setCellValue(bool);
		}
		else {
			cell.setBlank();
		}
	}

}