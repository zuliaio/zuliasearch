package io.zulia.data.source.spreadsheet.excel;

import io.zulia.data.source.spreadsheet.CellParsers;
import io.zulia.util.BooleanUtil;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;

import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Objects;
import java.util.function.Function;


public class DefaultExcelCellHandler implements ExcelCellHandler {

	private final CellParsers parsers;

	public DefaultExcelCellHandler() {
		this(CellParsers.defaults());
	}

	public DefaultExcelCellHandler(CellParsers parsers) {
		this.parsers = Objects.requireNonNull(parsers, "parsers");
	}

	@Override
	public String cellToString(Cell cell) {

		if (isCellString(cell)) {
			return cell.getStringCellValue();
		}
		else if (isCellNumeric(cell)) {
			if (isCellDateFormatted(cell)) {
				return formatDateCellAsString(cell);
			}
			return formatNumericCellAsString(cell);

		}
		else if (isCellBoolean(cell)) {
			return String.valueOf(cell.getBooleanCellValue());
		}
		else if (isCellFormula(cell)) {
			CellType cachedFormulaResultType = cell.getCachedFormulaResultType();
			if (cachedFormulaResultType.equals(CellType.NUMERIC)) {
				return formatNumericCellAsString(cell);
			}
			else if (cachedFormulaResultType.equals(CellType.STRING)) {
				return cell.getRichStringCellValue().getString();
			}
			else if (cachedFormulaResultType.equals(CellType.BOOLEAN)) {
				return String.valueOf(cell.getBooleanCellValue());
			}
		}

		return null;
	}

	@Override
	public Boolean cellToBoolean(Cell cell) {

		if (isCellBoolean(cell)) {
			return cell.getBooleanCellValue();
		}
		else if (isCellString(cell)) {
			return parseText(cell.getStringCellValue(), parsers.booleanParser());
		}
		else if (isCellNumeric(cell)) {
			return BooleanUtil.parseBoolean(cell.getNumericCellValue());
		}
		else if (isCellFormula(cell)) {
			CellType cachedFormulaResultType = cell.getCachedFormulaResultType();
			if (cachedFormulaResultType.equals(CellType.BOOLEAN)) {
				return cell.getBooleanCellValue();
			}
			else if (cachedFormulaResultType.equals(CellType.STRING)) {
				return parseText(cell.getRichStringCellValue().getString(), parsers.booleanParser());
			}
			else if (cachedFormulaResultType.equals(CellType.NUMERIC)) {
				return BooleanUtil.parseBoolean(cell.getNumericCellValue());
			}
		}

		return null;
	}

	@Override
	public Integer cellToInt(Cell cell) {

		if (isCellNumeric(cell)) {
			return Math.toIntExact((long) cell.getNumericCellValue());
		}
		else if (isCellFormula(cell)) {
			if (cell.getCachedFormulaResultType().equals(CellType.NUMERIC)) {
				return Math.toIntExact((long) cell.getNumericCellValue());
			}
		}
		else if (isCellString(cell)) {
			return parseText(cell.getStringCellValue(), Integer::parseInt);
		}

		return null;
	}

	@Override
	public Long cellToLong(Cell cell) {

		if (isCellNumeric(cell)) {
			return (long) cell.getNumericCellValue();
		}
		else if (isCellFormula(cell)) {
			if (cell.getCachedFormulaResultType().equals(CellType.NUMERIC)) {
				return (long) cell.getNumericCellValue();
			}
		}
		else if (isCellString(cell)) {
			return parseText(cell.getStringCellValue(), Long::parseLong);
		}

		return null;
	}

	@Override
	public Float cellToFloat(Cell cell) {

		if (isCellNumeric(cell)) {
			return (float) cell.getNumericCellValue();
		}
		else if (isCellFormula(cell)) {
			if (cell.getCachedFormulaResultType().equals(CellType.NUMERIC)) {
				return (float) cell.getNumericCellValue();
			}
		}
		else if (isCellString(cell)) {
			return parseText(cell.getStringCellValue(), Float::parseFloat);
		}

		return null;
	}

	@Override
	public Double cellToDouble(Cell cell) {
		if (isCellNumeric(cell)) {
			return cell.getNumericCellValue();
		}
		else if (isCellFormula(cell)) {
			if (cell.getCachedFormulaResultType().equals(CellType.NUMERIC)) {
				return cell.getNumericCellValue();
			}
		}
		else if (isCellString(cell)) {
			return parseText(cell.getStringCellValue(), Double::parseDouble);
		}

		return null;
	}

	@Override
	public Date cellToDate(Cell cell) {

		if (isCellNumeric(cell)) {
			return cell.getDateCellValue();
		}
		else if (isCellFormula(cell)) {
			CellType cachedFormulaResultType = cell.getCachedFormulaResultType();
			if (cachedFormulaResultType.equals(CellType.NUMERIC)) {
				return cell.getDateCellValue();
			}
			else if (cachedFormulaResultType.equals(CellType.STRING)) {
				return parseText(cell.getRichStringCellValue().getString(), parsers.dateParser());
			}
		}
		else if (isCellString(cell)) {
			return parseText(cell.getStringCellValue(), parsers.dateParser());
		}

		return null;
	}

	/**
	 * Applies the parser to the trimmed text, or returns null when the text is blank.
	 */
	private static <T> T parseText(String text, Function<String, T> parser) {
		String trimmed = text.trim();
		return trimmed.isEmpty() ? null : parser.apply(trimmed);
	}

	@Override
	public boolean isCellNumeric(Cell cell) {
		return cell != null && cell.getCellType().equals(CellType.NUMERIC);
	}

	@Override
	public boolean isCellString(Cell cell) {
		return cell != null && cell.getCellType().equals(CellType.STRING);
	}

	@Override
	public boolean isCellBoolean(Cell cell) {
		return cell != null && cell.getCellType().equals(CellType.BOOLEAN);
	}

	@Override
	public boolean isCellFormula(Cell cell) {
		return cell != null && cell.getCellType().equals(CellType.FORMULA);
	}

	@Override
	public boolean isCellDateFormatted(Cell cell) {
		return DateUtil.isCellDateFormatted(cell);
	}

	@Override
	public String formatNumericCellAsString(Cell cell) {
		double value = cell.getNumericCellValue();
		// upper bound is strict because 2^63 is a valid double but overflows long
		if (value == Math.floor(value) && !Double.isInfinite(value) && value >= -0x1p63 && value < 0x1p63) {
			return String.valueOf((long) value);
		}
		return String.valueOf(value);
	}

	@Override
	public String formatDateCellAsString(Cell cell) {
		return DateTimeFormatter.ISO_INSTANT.format(cell.getDateCellValue().toInstant());
	}
}
