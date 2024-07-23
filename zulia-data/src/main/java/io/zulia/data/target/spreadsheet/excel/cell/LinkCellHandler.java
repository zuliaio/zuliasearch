package io.zulia.data.target.spreadsheet.excel.cell;

import io.zulia.data.target.spreadsheet.excel.WorkbookHelper;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.Hyperlink;
import org.apache.poi.xssf.streaming.SXSSFCell;
import org.apache.poi.xssf.usermodel.XSSFFont;

public class LinkCellHandler implements CellHandler<Link> {

	@Override
	public void handleCell(WorkbookHelper workbookHelper, SXSSFCell cell, Link link) {
		if (link != null) {
			CellStyle linkStyle = workbookHelper.createOrGetStyle("linkStyle", style -> {
				Font linkFont = workbookHelper.createOrGetFont("linkFont", font -> {
					font.setUnderline(XSSFFont.U_SINGLE);
					font.setColor(HSSFColor.HSSFColorPredefined.BLUE.getIndex());
				});
				style.setFont(linkFont);
			});
			cell.setCellStyle(linkStyle);
			Hyperlink hyperlink = workbookHelper.createLink(link.href());
			cell.setHyperlink(hyperlink);
			cell.setCellValue(link.label());
		}
		else {
			cell.setBlank();
		}
	}
}
