package io.zulia.data.source.spreadsheet;

import io.zulia.data.source.DataSourceRecord;

import java.util.Date;
import java.util.List;
import java.util.function.Function;

public interface SpreadsheetRecord extends DataSourceRecord {
	<T> List<T> getList(int index, final Class<T> clazz);

	String getString(int index);

	default String getString(int index, String defaultValue) {
		String val = getString(index);
		return val != null ? val : defaultValue;
	}

	default <T> T parseFromString(String field, Function<String, T> parser, T defaultValue) {
		String strVal = getString(field);
		if (strVal == null) {
			return defaultValue;
		}
		return parser.apply(strVal);
	}

	default <T> T parseFromString(int index, Function<String, T> parser, T defaultValue) {
		String strVal = getString(index);
		if (strVal == null) {
			return defaultValue;
		}
		return parser.apply(strVal);
	}

	Boolean getBoolean(int index);

	default boolean getBoolean(int index, boolean defaultValue) {
		Boolean val = getBoolean(index);
		return val != null ? val : defaultValue;
	}

	Float getFloat(int index);

	default float getFloat(int index, float defaultValue) {
		Float val = getFloat(index);
		return val != null ? val : defaultValue;
	}

	Double getDouble(int index);

	default double getDouble(int index, double defaultValue) {
		Double val = getDouble(index);
		return val != null ? val : defaultValue;
	}

	Integer getInt(int index);

	default int getDouble(int index, int defaultValue) {
		Integer val = getInt(index);
		return val != null ? val : defaultValue;
	}

	Long getLong(int index);

	default long getLong(int index, long defaultValue) {
		Long val = getLong(index);
		return val != null ? val : defaultValue;
	}

	Date getDate(int index);

	default Date getDate(int index, Date defaultValue) {
		if (defaultValue == null) {
			throw new IllegalArgumentException("defaultValue cannot be null");
		}
		Date val = getDate(index);
		return val != null ? val : defaultValue;
	}

	String[] getRow();

}
