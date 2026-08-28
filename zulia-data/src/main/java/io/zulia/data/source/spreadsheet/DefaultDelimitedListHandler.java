package io.zulia.data.source.spreadsheet;

import com.google.common.base.Splitter;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Splits a cell on the list delimiter and converts each element to the requested class.
 * Numeric elements use the JDK parsers. Date and Boolean elements use the {@link CellParsers} of the source config, so they follow
 * the same rules as {@link SpreadsheetRecord#getDate(int)} and {@link SpreadsheetRecord#getBoolean(int)}. For every class but
 * String an element is trimmed before it is converted and a blank element becomes a null placeholder, so positions line up with
 * sibling list columns and {@link #collectionToCellValue} writes the list back the way it was read. A blank whole cell reads as an
 * empty list, matching how a blank whole cell reads as null for a single value. String lists keep every element as written.
 */
public class DefaultDelimitedListHandler implements DelimitedListHandler {

	private final char listDelimiter;
	private final Map<Class<?>, Function<String, ?>> converters;
	private final Function<Date, String> dateFormatter;

	public DefaultDelimitedListHandler(char listDelimiter) {
		this(listDelimiter, CellParsers.defaults());
	}

	public DefaultDelimitedListHandler(char listDelimiter, CellParsers parsers) {
		this.listDelimiter = listDelimiter;
		this.dateFormatter = parsers.dateFormatter();
		this.converters = Map.of(
				String.class, Function.identity(),
				Integer.class, Integer::parseInt,
				Long.class, Long::parseLong,
				Float.class, Float::parseFloat,
				Double.class, Double::parseDouble,
				Date.class, parsers.dateParser(),
				Boolean.class, parsers.booleanParser());
	}

	@Override
	public <T> List<T> cellValueToList(Class<T> clazz, String cellValue) {
		if (cellValue == null) {
			return null;
		}
		Function<String, ?> converter = converters.get(clazz);
		if (converter == null) {
			throw new IllegalArgumentException("Unsupported class " + clazz);
		}
		if (clazz.equals(String.class)) {
			return Splitter.on(listDelimiter).splitToStream(cellValue).map(clazz::cast).toList();
		}
		if (cellValue.isBlank()) {
			return List.of();
		}
		Stream<String> elements = Splitter.on(listDelimiter).splitToStream(cellValue).map(String::trim);
		return elements.map(element -> element.isEmpty() ? null : converter.apply(element)).map(clazz::cast).toList();
	}

	/**
	 * Joins the elements with the list delimiter. A null element is written as a blank, so it reads back as a null placeholder,
	 * and a Date is written with the date formatter so it reads back through the date parser. Other elements use toString,
	 * which the numeric parsers and the default boolean parser read.
	 */
	@Override
	public String collectionToCellValue(Collection<?> collection) {
		return collection.stream().map(this::elementToText).collect(Collectors.joining(String.valueOf(listDelimiter)));
	}

	private String elementToText(Object element) {
		return switch (element) {
			case null -> "";
			case Date date -> dateFormatter.apply(date);
			default -> element.toString();
		};
	}
}
