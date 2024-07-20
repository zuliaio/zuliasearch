package io.zulia.data.target.spreadsheet.excel.cell;

import io.zulia.data.target.spreadsheet.excel.WorkbookHelper;
import org.apache.poi.xssf.streaming.SXSSFCell;

public class NumberCellHandler implements CellHandler<Number> {

	@Override
	public void handleCell(WorkbookHelper workbookHelper, SXSSFCell cell, Number number) {
		if (number != null) {
			cell.setCellValue(number.doubleValue());
		}
		else {
			cell.setBlank();
		}
	}

}