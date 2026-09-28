package io.zulia.data.target.spreadsheet.excel.cell;

import io.zulia.data.target.spreadsheet.DateTypeHandler;
import org.apache.poi.ss.usermodel.BuiltinFormats;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.xssf.streaming.SXSSFCell;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;

/**
 * Writes a Date or a LocalDate as a typed Excel date cell in m/d/yy and a LocalDateTime in m/d/yy h:mm. A local value has no
 * zone, so POI stores the serial for that day or wall clock time regardless of the thread's user time zone. POI blanks the cell
 * for a null value.
 */
public class DateCellHandler implements DateTypeHandler<CellReference> {

	@Override
	public void writeType(CellReference reference, Date value) {
		SXSSFCell cell = reference.cell();
		if (value != null) {
			cell.setCellStyle(style(reference, "dateStyle", "m/d/yy"));
		}
		cell.setCellValue(value);
	}

	@Override
	public void writeType(CellReference reference, LocalDate value) {
		SXSSFCell cell = reference.cell();
		if (value != null) {
			cell.setCellStyle(style(reference, "dateStyle", "m/d/yy"));
		}
		cell.setCellValue(value);
	}

	@Override
	public void writeType(CellReference reference, LocalDateTime value) {
		SXSSFCell cell = reference.cell();
		if (value != null) {
			cell.setCellStyle(style(reference, "dateTimeStyle", "m/d/yy h:mm"));
		}
		cell.setCellValue(value);
	}

	private static CellStyle style(CellReference reference, String name, String builtinFormat) {
		return reference.workbookHelper().createOrGetStyle(name, style -> style.setDataFormat((short) BuiltinFormats.getBuiltinFormat(builtinFormat)));
	}
}
