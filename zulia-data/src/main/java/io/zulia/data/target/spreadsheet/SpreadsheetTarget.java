package io.zulia.data.target.spreadsheet;

import io.zulia.data.target.spreadsheet.excel.cell.Link;

import java.io.IOException;
import java.lang.reflect.Array;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

public abstract class SpreadsheetTarget<R, C extends SpreadsheetTargetConfig<?, ?>> implements AutoCloseable {

	private final SpreadsheetTargetConfig<R, C> dataConfig;

	public SpreadsheetTarget(SpreadsheetTargetConfig<R, C> dataConfig) {
		this.dataConfig = dataConfig;
	}

	public void appendValue(Collection<?> value) {
		dataConfig.getCollectionHandler().writeType(generateReference(), value);
	}

	public void appendValue(Map<?, ?> value) {
		dataConfig.getMapHandler().writeType(generateReference(), value);
	}

	public void appendValue(Boolean value) {
		dataConfig.getBooleanTypeHandler().writeType(generateReference(), value);
	}

	public void appendValue(Date value) {
		dataConfig.getDateTypeHandler().writeType(generateReference(), value);
	}

	public void appendValue(LocalDate value) {
		dataConfig.getDateTypeHandler().writeType(generateReference(), value);
	}

	public void appendValue(LocalDateTime value) {
		dataConfig.getDateTypeHandler().writeType(generateReference(), value);
	}

	public void appendValue(Number value) {
		dataConfig.getNumberTypeHandler().writeType(generateReference(), value);
	}

	public void appendValue(String value) {
		dataConfig.getStringTypeHandler().writeType(generateReference(), value);
	}

	public void appendValue(Link link) {
		dataConfig.getLinkTypeHandler().writeType(generateReference(), link);
	}

	public void appendGenericValue(Object o) {
		dataConfig.getDefaultTypeHandler().writeType(generateReference(), o);
	}

	public void appendValue(SpreadsheetTypeHandler<R, Collection<?>> spreadsheetTypeHandler, Collection<?> value) {
		spreadsheetTypeHandler.writeType(generateReference(), value);
	}

	public void appendValue(SpreadsheetTypeHandler<R, Map<?, ?>> spreadsheetTypeHandler, Map<?, ?> value) {
		spreadsheetTypeHandler.writeType(generateReference(), value);
	}

	public void appendValue(SpreadsheetTypeHandler<R, Boolean> spreadsheetTypeHandler, Boolean value) {
		spreadsheetTypeHandler.writeType(generateReference(), value);
	}

	public void appendValue(SpreadsheetTypeHandler<R, Date> spreadsheetTypeHandler, Date value) {
		spreadsheetTypeHandler.writeType(generateReference(), value);
	}

	public void appendValue(DateTypeHandler<R> dateTypeHandler, LocalDate value) {
		dateTypeHandler.writeType(generateReference(), value);
	}

	public void appendValue(DateTypeHandler<R> dateTypeHandler, LocalDateTime value) {
		dateTypeHandler.writeType(generateReference(), value);
	}

	public void appendValue(SpreadsheetTypeHandler<R, Number> spreadsheetTypeHandler, Number value) {
		spreadsheetTypeHandler.writeType(generateReference(), value);
	}

	public void appendValue(SpreadsheetTypeHandler<R, String> spreadsheetTypeHandler, String value) {
		spreadsheetTypeHandler.writeType(generateReference(), value);
	}

	public void appendValue(SpreadsheetTypeHandler<R, Link> spreadsheetTypeHandler, Link value) {
		spreadsheetTypeHandler.writeType(generateReference(), value);
	}

	public void appendGenericValue(SpreadsheetTypeHandler<R, Object> spreadsheetTypeHandler, Object o) {
		spreadsheetTypeHandler.writeType(generateReference(), o);
	}

	protected void writeHeaders(Collection<String> headers) {
		SpreadsheetTypeHandler<R, String> headerCellHandler = dataConfig.getHeaderHandler();
		for (String header : headers) {
			headerCellHandler.writeType(generateReference(), header);
		}
		finishRow();
	}

	protected abstract R generateReference();

	public void appendLink(String label, String href) {
		appendValue(new Link(label, href));
	}

	public void appendValue(Object o) {
		switch (o) {
			case Collection<?> collection -> appendValue(collection);
			case Map<?, ?> map -> appendValue(map);
			case Object array when array.getClass().isArray() -> appendValue(arrayAsList(array));
			case Date date -> appendValue(date);
			case LocalDate localDate -> appendValue(localDate);
			case LocalDateTime localDateTime -> appendValue(localDateTime);
			// a point in time is what a Date is, only the zone or offset it was expressed in is dropped
			case Instant instant -> appendValue(Date.from(instant));
			case ZonedDateTime zoned -> appendValue(Date.from(zoned.toInstant()));
			case OffsetDateTime offset -> appendValue(Date.from(offset.toInstant()));
			case Number number -> appendValue(number);
			case Boolean bool -> appendValue(bool);
			case Link link -> appendValue(link);
			case String string -> appendValue(string);
			case null, default -> appendGenericValue(o);
		}
	}

	/**
	 * Reflective so a primitive array unpacks too, which Arrays.asList cannot do.
	 */
	private static List<Object> arrayAsList(Object array) {
		int length = Array.getLength(array);
		List<Object> list = new ArrayList<>(length);
		for (int i = 0; i < length; i++) {
			list.add(Array.get(array, i));
		}
		return list;
	}

	public void appendValues(String... values) {
		for (String value : values) {
			appendValue(value);
		}
	}

	public void writeRow(String... values) {
		for (String value : values) {
			appendValue(value);
		}
		finishRow();
	}

	public void writeRow(Object... values) {
		for (Object value : values) {
			appendValue(value);
		}
		finishRow();
	}

	public void writeRow(Collection<?> values) {
		for (Object value : values) {
			appendValue(value);
		}
		finishRow();
	}

	public abstract void finishRow();

	public abstract void close() throws IOException;
}
