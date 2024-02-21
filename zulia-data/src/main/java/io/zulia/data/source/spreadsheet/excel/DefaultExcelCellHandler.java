package io.zulia.data.source.spreadsheet.excel;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;

import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DefaultExcelCellHandler implements ExcelCellHandler {
	@Override
	public String cellToString(Cell cell) {
		if (cell != null) {
			if (cell.getCellType().equals(CellType.STRING)) {
				return cell.getStringCellValue();
			}
			else if (cell.getCellType().equals(CellType.NUMERIC)) {
				if (DateUtil.isCellDateFormatted(cell)) {
					return DateTimeFormatter.ISO_INSTANT.format(cell.getDateCellValue().toInstant());
				}
				Number numericCellValue = cell.getNumericCellValue();
				if ((numericCellValue.doubleValue() == Math.floor(numericCellValue.doubleValue())) && !Double.isInfinite(numericCellValue.doubleValue())) {
					return String.valueOf(numericCellValue.intValue());
				}
				else {
					return String.valueOf(numericCellValue.doubleValue());
				}

			}
			else if (cell.getCellType().equals(CellType.FORMULA)) {
				if (cell.getCachedFormulaResultType().equals(CellType.NUMERIC)) {
					Number numericCellValue = cell.getNumericCellValue();
					if ((numericCellValue.doubleValue() == Math.floor(numericCellValue.doubleValue())) && !Double.isInfinite(numericCellValue.doubleValue())) {
						return String.valueOf(numericCellValue.intValue());
					}
					else {
						return String.valueOf(numericCellValue.doubleValue());
					}
				}
				else if (cell.getCachedFormulaResultType().equals(CellType.STRING)) {
					return cell.getRichStringCellValue().getString();
				}
			}

		}
		return null;
	}

	@Override
	public Boolean cellToBoolean(Cell cell) {
		if (cell != null) {
			if (cell.getCellType().equals(CellType.BOOLEAN)) {
				return cell.getBooleanCellValue();
			}
			else if (cell.getCellType().equals(CellType.STRING)) {
				return Boolean.parseBoolean(cell.getStringCellValue());
			}
		}
		return null;
	}

	@Override
	public Integer cellToInt(Cell cell) {
		if (cell != null) {
			if (cell.getCellType().equals(CellType.NUMERIC)) {
				return (int) cell.getNumericCellValue();
			}
			else if (cell.getCellType().equals(CellType.FORMULA)) {
				if (cell.getCachedFormulaResultType().equals(CellType.NUMERIC)) {
					return (int) cell.getNumericCellValue();
				}
			}
			else if (cell.getCellType().equals(CellType.STRING)) {
				return Integer.parseInt(cell.getStringCellValue());
			}
		}
		return null;
	}

	@Override
	public Long cellToLong(Cell cell) {
		if (cell != null) {
			if (cell.getCellType().equals(CellType.NUMERIC)) {
				return (long) cell.getNumericCellValue();
			}
			else if (cell.getCellType().equals(CellType.FORMULA)) {
				if (cell.getCachedFormulaResultType().equals(CellType.NUMERIC)) {
					return (long) cell.getNumericCellValue();
				}
			}
			else if (cell.getCellType().equals(CellType.STRING)) {
				return Long.parseLong(cell.getStringCellValue());
			}
		}
		return null;
	}

	@Override
	public Float cellToFloat(Cell cell) {
		if (cell != null) {
			if (cell.getCellType().equals(CellType.NUMERIC)) {
				return (float) cell.getNumericCellValue();
			}
			else if (cell.getCellType().equals(CellType.FORMULA)) {
				if (cell.getCachedFormulaResultType().equals(CellType.NUMERIC)) {
					return (float) cell.getNumericCellValue();
				}
			}
			else if (cell.getCellType().equals(CellType.STRING)) {
				return Float.parseFloat(cell.getStringCellValue());
			}
		}
		return null;
	}

	@Override
	public Double cellToDouble(Cell cell) {
		if (cell != null) {
			if (cell.getCellType().equals(CellType.NUMERIC)) {
				return cell.getNumericCellValue();
			}
			else if (cell.getCellType().equals(CellType.FORMULA)) {
				if (cell.getCachedFormulaResultType().equals(CellType.NUMERIC)) {
					return cell.getNumericCellValue();
				}
			}
			else if (cell.getCellType().equals(CellType.STRING)) {
				return Double.parseDouble(cell.getStringCellValue());
			}
		}
		return null;
	}

	@Override
	public Date cellToDate(Cell cell) {
		if (cell != null) {
			if (cell.getCellType().equals(CellType.NUMERIC)) {
				return cell.getDateCellValue();
			}
		}
		return null;
	}
}
