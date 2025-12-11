package io.zulia.data.target.spreadsheet.delimited.formatter;

import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;

public class DateCSVWriter<T extends List<String>> implements SpreadsheetTypeHandler<T, Date> {

	private DateTimeFormatter dateTimeFormatter;

	public DateCSVWriter() {
		this(DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.systemDefault()));
	}

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

	@Override
	public void writeType(T reference, Date value) {
		if (value != null) {
			reference.add(dateTimeFormatter.format(value.toInstant()));
		}
		else {
			reference.add(null);
		}
	}
}
