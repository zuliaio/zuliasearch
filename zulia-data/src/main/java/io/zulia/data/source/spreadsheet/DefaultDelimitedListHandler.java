package io.zulia.data.source.spreadsheet;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public class DefaultDelimitedListHandler implements DelimitedListHandler {

	private final char listDelimiter;

	public DefaultDelimitedListHandler(char listDelimiter) {
		this.listDelimiter = listDelimiter;
	}

	public <T> List<T> cellValueToList(Class<T> clazz, String cellValue) {
		if (cellValue != null) {
			Stream<String> listStream = Splitter.on(listDelimiter).splitToStream(cellValue);
			if (clazz.equals(String.class)) {
				return (List<T>) listStream.toList();
			}
			else if (clazz.equals(Integer.class)) {
				return (List<T>) listStream.map(Integer::parseInt).toList();
			}
			else if (clazz.equals(Long.class)) {
				return (List<T>) listStream.map(Long::parseLong).toList();
			}
			else if (clazz.equals(Float.class)) {
				return (List<T>) listStream.map(Float::parseFloat).toList();
			}
			else if (clazz.equals(Double.class)) {
				return (List<T>) listStream.map(Double::parseDouble).toList();
			}
			else {
				throw new IllegalArgumentException("Unsupported clazz <" + clazz + ">");
			}
		}
		return null;
	}

	@Override
	public String collectionToCellValue(Collection<?> collection) {
		return Joiner.on(listDelimiter).join(collection);
	}
}
