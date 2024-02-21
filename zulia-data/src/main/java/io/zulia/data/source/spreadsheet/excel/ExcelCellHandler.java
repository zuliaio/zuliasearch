package io.zulia.data.source.spreadsheet.excel;

import org.apache.poi.ss.usermodel.Cell;

import java.util.Date;

public interface ExcelCellHandler {
	String cellToString(Cell cell);

	Boolean cellToBoolean(Cell cell);

	Integer cellToInt(Cell cell);

	Long cellToLong(Cell cell);

	Float cellToFloat(Cell cell);

	Double cellToDouble(Cell cell);

	Date cellToDate(Cell cell);
}
