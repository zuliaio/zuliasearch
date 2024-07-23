package io.zulia.data.target.spreadsheet.excel.cell;

import io.zulia.data.target.spreadsheet.excel.WorkbookHelper;
import org.apache.poi.ss.usermodel.BuiltinFormats;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.xssf.streaming.SXSSFCell;

import java.util.Date;

public class DateCellHandler implements CellHandler<Date> {

	@Override
	public void handleCell(WorkbookHelper workbookHelper, SXSSFCell cell, Date date) {
		if (date != null) {
			CellStyle dateStyle = workbookHelper.createOrGetStyle("dateStyle", style -> {
				style.setDataFormat((short) BuiltinFormats.getBuiltinFormat("m/d/yy"));
			});
			cell.setCellStyle(dateStyle);
			cell.setCellValue(date);
		}
		else {
			cell.setBlank();
		}
	}

}