package io.zulia.data.source.spreadsheet.excel;

import org.apache.poi.ss.usermodel.Cell;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;

public interface ExcelCellHandler {
	String cellToString(Cell cell);

	Boolean cellToBoolean(Cell cell);

	Integer cellToInt(Cell cell);

	Long cellToLong(Cell cell);

	Float cellToFloat(Cell cell);

	Double cellToDouble(Cell cell);

	Date cellToDate(Cell cell);

	LocalDate cellToLocalDate(Cell cell);

	LocalDateTime cellToLocalDateTime(Cell cell);

	boolean isCellNumeric(Cell cell);

	boolean isCellString(Cell cell);

	boolean isCellBoolean(Cell cell);

	boolean isCellFormula(Cell cell);

	boolean isCellDateFormatted(Cell cell);

	String formatNumericCellAsString(Cell cell);

	String formatDateCellAsString(Cell cell);
}
