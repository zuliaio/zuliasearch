package io.zulia.data.target.spreadsheet.csv.formatter;

import com.univocity.parsers.csv.CsvWriter;
import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateCSVWriter implements SpreadsheetTypeHandler<CsvWriter, Date> {
	
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
	
	public DateCSVWriter withDateTimeFormatter(DateTimeFormatter dateTimeFormatter) {
		this.dateTimeFormatter = dateTimeFormatter;
		return this;
	}
	
	@Override
	public void writeType(CsvWriter reference, Date value) {
		if (value != null) {
			reference.addValue(dateTimeFormatter.format(value.toInstant()));
		}
		else {
			reference.addValue(null);
		}
	}
}
