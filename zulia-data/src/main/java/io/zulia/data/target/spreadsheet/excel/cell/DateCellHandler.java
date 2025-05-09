package io.zulia.data.target.spreadsheet.excel.cell;

import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;
import org.apache.poi.ss.usermodel.BuiltinFormats;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.xssf.streaming.SXSSFCell;

import java.util.Date;

public class DateCellHandler implements SpreadsheetTypeHandler<CellReference, Date> {

	@Override
	public void writeType(CellReference reference, Date value) {
		SXSSFCell cell = reference.cell();
		if (value != null) {
			CellStyle dateStyle = reference.workbookHelper().createOrGetStyle("dateStyle", style -> {
				style.setDataFormat((short) BuiltinFormats.getBuiltinFormat("m/d/yy"));
			});
			cell.setCellStyle(dateStyle);
			cell.setCellValue(value);
		}
		else {
			cell.setBlank();
		}
	}
}