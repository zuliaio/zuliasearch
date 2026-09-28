package io.zulia.data.target.spreadsheet.delimited.formatter;

import io.zulia.data.target.spreadsheet.DateTypeHandler;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import java.util.List;

/**
 * Writes a Date, a LocalDate and a LocalDateTime, each with its own formatter. By default a Date is ISO date time in the system
 * zone, a LocalDate is an ISO local date such as 2024-05-01 and a LocalDateTime is an ISO local date time such as
 * 2024-05-01T13:45:30, with no offset since the local values have no zone.
 */
public class DateCSVWriter<T extends List<String>> implements DateTypeHandler<T> {

	private DateTimeFormatter dateTimeFormatter;
	private DateTimeFormatter localDateFormatter = DateTimeFormatter.ISO_LOCAL_DATE;
	private DateTimeFormatter localDateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

	public DateCSVWriter() {
		this(DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.systemDefault()));
	}

	/**
	 * The local formatters keep their ISO defaults and are set with the with methods.
	 */
	public DateCSVWriter(DateTimeFormatter dateTimeFormatter) {
		this.dateTimeFormatter = dateTimeFormatter;
	}

	public DateTimeFormatter getDateTimeFormatter() {
		return dateTimeFormatter;
	}

	public DateCSVWriter<T> withDateTimeFormatter(DateTimeFormatter dateTimeFormatter) {
		this.dateTimeFormatter = dateTimeFormatter;
		return this;
	}

	public DateTimeFormatter getLocalDateFormatter() {
		return localDateFormatter;
	}

	public DateCSVWriter<T> withLocalDateFormatter(DateTimeFormatter localDateFormatter) {
		this.localDateFormatter = localDateFormatter;
		return this;
	}

	public DateTimeFormatter getLocalDateTimeFormatter() {
		return localDateTimeFormatter;
	}

	public DateCSVWriter<T> withLocalDateTimeFormatter(DateTimeFormatter localDateTimeFormatter) {
		this.localDateTimeFormatter = localDateTimeFormatter;
		return this;
	}

	@Override
	public void writeType(T reference, Date value) {
		add(reference, value != null ? value.toInstant() : null, dateTimeFormatter);
	}

	@Override
	public void writeType(T reference, LocalDate value) {
		add(reference, value, localDateFormatter);
	}

	@Override
	public void writeType(T reference, LocalDateTime value) {
		add(reference, value, localDateTimeFormatter);
	}

	private static void add(List<String> reference, TemporalAccessor value, DateTimeFormatter formatter) {
		reference.add(value != null ? formatter.format(value) : null);
	}
}
