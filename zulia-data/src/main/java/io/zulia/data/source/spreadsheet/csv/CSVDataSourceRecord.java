package io.zulia.data.source.spreadsheet.csv;

import io.zulia.data.common.HeaderMapping;
import io.zulia.data.source.spreadsheet.DelimitedListHandler;
import io.zulia.data.source.spreadsheet.IndexedDataSourceRecord;

import java.util.Date;
import java.util.List;
import java.util.SequencedSet;

public class CSVDataSourceRecord implements IndexedDataSourceRecord {

	private final String[] row;
	private final HeaderMapping headerMapping;

	private final DelimitedListHandler delimitedListHandler;

	public CSVDataSourceRecord(String[] row, HeaderMapping headerMapping, CSVDataSourceConfig csvDataSourceConfig) {
		this.row = row;
		this.headerMapping = headerMapping;
		this.delimitedListHandler = csvDataSourceConfig.getDelimitedListHandler();
	}

	public int getIndexFromField(String field) {
		if (headerMapping == null) {
			throw new IllegalStateException("Use csvDataSourceConfig.withHeaders() use field names");
		}
		if (headerMapping.hasHeader(field)) {
			return headerMapping.getHeaderIndex(field);
		}
		throw new IllegalStateException("Field <" + field + "> does not exist in header");
	}

	@Override
	public <T> List<T> getList(String key, Class<T> clazz) {
		String cellValue = getString(key);
		return delimitedListHandler.cellValueToList(clazz, cellValue);
	}

	@Override
	public String getString(String field) {
		return row[getIndexFromField(field)];
	}

	@Override
	public Boolean getBoolean(String field) {
		return Boolean.parseBoolean(getString(field));
	}

	@Override
	public Float getFloat(String field) {
		return Float.parseFloat(getString(field));
	}

	@Override
	public Double getDouble(String field) {
		return Double.parseDouble(getString(field));
	}

	@Override
	public Integer getInt(String field) {
		return Integer.parseInt(getString(field));
	}

	@Override
	public Long getLong(String field) {
		return Long.parseLong(getString(field));
	}

	@Override
	public Date getDate(String field) {
		return null;
	}

	@Override
	public <T> List<T> getList(int index, Class<T> clazz) {
		String cellValue = getString(index);
		return delimitedListHandler.cellValueToList(clazz, cellValue);
	}

	@Override
	public String getString(int index) {
		return row[index];
	}

	@Override
	public Boolean getBoolean(int index) {
		return Boolean.parseBoolean(getString(index));
	}

	@Override
	public Float getFloat(int index) {
		return Float.parseFloat(getString(index));
	}

	@Override
	public Double getDouble(int index) {
		return Double.parseDouble(getString(index));
	}

	@Override
	public Integer getInt(int index) {
		return Integer.parseInt(getString(index));
	}

	@Override
	public Long getLong(int index) {
		return Long.parseLong(getString(index));
	}

	@Override
	public Date getDate(int index) {
		return null;
	}

	public String[] getRow() {
		return row;
	}

	public SequencedSet<String> getHeaders() {
		return headerMapping != null ? headerMapping.getHeaderKeys() : null;
	}

	public List<String> getRawHeaders() {
		return headerMapping != null ? headerMapping.getRawHeaders() : null;
	}

}
