package io.zulia.data.target.spreadsheet.excel.cell;

import io.zulia.data.target.spreadsheet.excel.WorkbookHelper;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.xssf.streaming.SXSSFCell;

public class HeaderCellHandler implements CellHandler<String> {

	@Override
	public void handleCell(WorkbookHelper workbookHelper, SXSSFCell cell, String s) {
		if (s != null) {
			CellStyle linkStyle = workbookHelper.createOrGetStyle("headerBoldStyle", style -> {
				Font headerBoldFont = workbookHelper.createOrGetFont("headerBoldFont", font -> {
					font.setBold(true);
					font.setFontHeightInPoints((short) 10);
				});
				style.setAlignment(HorizontalAlignment.CENTER);
				style.setFont(headerBoldFont);
			});
			cell.setCellStyle(linkStyle);
			cell.setCellValue(s);
		}
		else {
			cell.setBlank();
		}
	}

}