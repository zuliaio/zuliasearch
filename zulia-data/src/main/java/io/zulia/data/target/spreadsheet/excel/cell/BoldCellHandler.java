package io.zulia.data.target.spreadsheet.excel.cell;

import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;
import io.zulia.data.target.spreadsheet.excel.WorkbookHelper;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.xssf.streaming.SXSSFCell;

public class BoldCellHandler implements SpreadsheetTypeHandler<CellReference, String> {

	@Override
	public void writeType(CellReference reference, String value) {
		SXSSFCell cell = reference.cell();
		if (value != null) {
			WorkbookHelper workbookHelper = reference.workbookHelper();
			CellStyle linkStyle = workbookHelper.createOrGetStyle("boldStyle", style -> {
				Font boldFont = workbookHelper.createOrGetFont("boldFont", font -> {
					font.setBold(true);
					font.setFontHeightInPoints((short) 10);
				});
				style.setFont(boldFont);
			});
			cell.setCellStyle(linkStyle);
			cell.setCellValue(value);
		}
		else {
			cell.setBlank();
		}
	}
}