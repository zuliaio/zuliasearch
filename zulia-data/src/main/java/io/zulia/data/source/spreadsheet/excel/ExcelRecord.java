package io.zulia.data.source.spreadsheet.excel;

import io.zulia.data.common.HeaderMapping;
import io.zulia.data.source.spreadsheet.DelimitedListHandler;
import io.zulia.data.source.spreadsheet.SpreadsheetRecord;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;

import java.util.Date;
import java.util.List;
import java.util.SequencedSet;

public class ExcelRecord implements SpreadsheetRecord {

	private final Row row;

	private final DelimitedListHandler delimitedListHandler;
	private final ExcelCellHandler excelCellHandler;
	private final SheetInfo sheetInfo;

	public ExcelRecord(Row row, SheetInfo sheetInfo, ExcelSourceConfig excelSourceConfig) {
		this.row = row;
		this.sheetInfo = sheetInfo;
		this.delimitedListHandler = excelSourceConfig.getDelimitedListHandler();
		this.excelCellHandler = excelSourceConfig.getExcelCellHandler();
	}

	public int getIndexFromField(String field) {
		HeaderMapping headerMapping = sheetInfo.headerMapping();
		if (headerMapping == null) {
			throw new IllegalStateException("Use excelDataSourceConfig.withHeaders() use field names");
		}
		if (headerMapping.hasHeader(field)) {
			return headerMapping.getHeaderIndex(field);
		}
		throw new IllegalStateException("Field " + field + " does not exist in header");
	}

	public SequencedSet<String> getHeaders() {
		return sheetInfo.headerMapping() != null ? sheetInfo.headerMapping().getHeaderKeys() : null;
	}

	public List<String> getRawHeaders() {
		return sheetInfo.headerMapping() != null ? sheetInfo.headerMapping().getRawHeaders() : null;
	}

	public Cell getCell(int i) {
		return row.getCell(i);
	}

	public Cell getCell(String field) {
		return getCell(getIndexFromField(field));
	}

	@Override
	public <T> List<T> getList(String key, Class<T> clazz) {
		String cellValue = getString(key);
		return delimitedListHandler.cellValueToList(clazz, cellValue);
	}

	@Override
	public String getString(String field) {
		Cell cell = getCell(field);
		return excelCellHandler.cellToString(cell);
	}

	@Override
	public Boolean getBoolean(String field) {
		return excelCellHandler.cellToBoolean(getCell(field));
	}

	@Override
	public Float getFloat(String field) {
		return excelCellHandler.cellToFloat(getCell(field));
	}

	@Override
	public Double getDouble(String field) {
		return excelCellHandler.cellToDouble(getCell(field));
	}

	@Override
	public Integer getInt(String field) {
		return excelCellHandler.cellToInt(getCell(field));
	}

	@Override
	public Long getLong(String field) {
		return excelCellHandler.cellToLong(getCell(field));
	}

	@Override
	public Date getDate(String field) {
		return excelCellHandler.cellToDate(getCell(field));
	}

	@Override
	public <T> List<T> getList(int index, Class<T> clazz) {
		String cellValue = getString(index);
		return delimitedListHandler.cellValueToList(clazz, cellValue);
	}

	@Override
	public String getString(int index) {
		return excelCellHandler.cellToString(row.getCell(index));
	}

	@Override
	public Boolean getBoolean(int index) {
		return excelCellHandler.cellToBoolean(row.getCell(index));
	}

	@Override
	public Float getFloat(int index) {
		return excelCellHandler.cellToFloat(row.getCell(index));
	}

	@Override
	public Double getDouble(int index) {
		return excelCellHandler.cellToDouble(row.getCell(index));
	}

	@Override
	public Integer getInt(int index) {
		return excelCellHandler.cellToInt(row.getCell(index));
	}

	@Override
	public Long getLong(int index) {
		return excelCellHandler.cellToLong(row.getCell(index));
	}

	@Override
	public Date getDate(int index) {
		return excelCellHandler.cellToDate(row.getCell(index));
	}

	public String[] getRow() {
		String[] values = new String[sheetInfo.numberOfColumns()];
		for (int i = 0; i < sheetInfo.numberOfColumns(); i++) {
			Cell cell = row.getCell(i);
			values[i] = excelCellHandler.cellToString(cell);
		}
		return values;
	}

	public Row getNativeRow() {
		return row;
	}

}
