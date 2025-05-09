package io.zulia.data.target.spreadsheet.excel.cell;

import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;

public class StringCellHandler implements SpreadsheetTypeHandler<CellReference, String> {

	public static String truncateForExcel(String cell) {
		return cell != null && cell.length() > 32000 ? cell.substring(0, 32000) + "...[TRUNCATED]" : cell;
	}

	@Override
	public void writeType(CellReference reference, String value) {
		reference.cell().setCellValue(truncateForExcel(value));
	}
}