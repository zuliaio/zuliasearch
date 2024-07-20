package io.zulia.data.target.spreadsheet.excel.cell;

import io.zulia.data.target.spreadsheet.excel.WorkbookHelper;
import org.apache.poi.xssf.streaming.SXSSFCell;

public class StringCellHandler implements CellHandler<String> {

	@Override
	public void handleCell(WorkbookHelper workbookHelper, SXSSFCell cell, String string) {
		cell.setCellValue(truncateForExcel(string));
	}

	public static String truncateForExcel(String cell) {
		return cell != null && cell.length() > 32000 ? cell.substring(0, 32000) + "...[TRUNCATED]" : cell;
	}
}