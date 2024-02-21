package io.zulia.data.source;

import java.util.Date;
import java.util.List;

public interface DataSourceRecord {

	<T> List<T> getList(String field, final Class<T> clazz);

	String getString(String field);

	default String getString(String field, String defaultValue) {
		String val = getString(field);
		return val != null ? val : defaultValue;
	}

	Boolean getBoolean(String field);

	default boolean getBoolean(String field, boolean defaultValue) {
		Boolean val = getBoolean(field);
		return val != null ? val : defaultValue;
	}

	Float getFloat(String field);

	default float getFloat(String field, float defaultValue) {
		Float val = getFloat(field);
		return val != null ? val : defaultValue;
	}

	Double getDouble(String field);

	default double getDouble(String field, double defaultValue) {
		Double val = getDouble(field);
		return val != null ? val : defaultValue;
	}

	Integer getInt(String field);

	default int getDouble(String field, int defaultValue) {
		Integer val = getInt(field);
		return val != null ? val : defaultValue;
	}

	Long getLong(String field);

	default long getLong(String field, long defaultValue) {
		Long val = getLong(field);
		return val != null ? val : defaultValue;
	}

	Date getDate(String field);

	default Date getDate(String field, Date defaultValue) {
		if (defaultValue == null) {
			throw new IllegalArgumentException("defaultValue cannot be null");
		}
		Date val = getDate(field);
		return val != null ? val : defaultValue;
	}

}
