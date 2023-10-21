package io.zulia.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;

public class ZuliaDateUtil {

	public static final DateTimeFormatter YEAR_MONTH_DAY_TIME = new DateTimeFormatterBuilder().appendValue(ChronoField.YEAR, 4).appendLiteral('-')
			.appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NEVER).appendLiteral('-').appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NEVER)
			.appendLiteral('T').append(ISO_LOCAL_TIME).appendOffsetId().toFormatter();

	public static final DateTimeFormatter YEAR_MONTH_DAY = new DateTimeFormatterBuilder().appendValue(ChronoField.YEAR, 4).appendLiteral('-')
			.appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NEVER).appendLiteral('-').appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NEVER)
			.toFormatter();

	public static final DateTimeFormatter YEAR_MONTH = new DateTimeFormatterBuilder().appendValue(ChronoField.YEAR, 4).appendLiteral('-')
			.appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NEVER).toFormatter();

	public static final DateTimeFormatter YEAR_ONLY = new DateTimeFormatterBuilder().appendValue(ChronoField.YEAR, 4).toFormatter();

	public record DateBounds(long begin, long end) {

	}

	public static DateBounds getParseDate(String dateString) {

		if (dateString.indexOf('/') != -1) {
			dateString = dateString.replace('/', '-');
		}

		if (dateString.contains(":")) {

			try {
				long epochMilli = YEAR_MONTH_DAY_TIME.parse(dateString, Instant::from).toEpochMilli();
				// if contains timestamp there is no range
				return new DateBounds(epochMilli, epochMilli);
			}
			catch (DateTimeParseException e) {
				System.out.println(e.getMessage());
				return null;
			}
		}
		else {
			LocalDate date;
			try {
				date = LocalDate.parse(dateString, YEAR_MONTH_DAY);
				LocalDateTime startOfDayOnDate = date.atStartOfDay();
				long begin = startOfDayOnDate.toInstant(ZoneOffset.UTC).toEpochMilli();
				long end = startOfDayOnDate.plusDays(1).toInstant(ZoneOffset.UTC).toEpochMilli() - 1;
				return new DateBounds(begin, end);
			}
			catch (DateTimeParseException ignored) {
			}
			try {
				date = YearMonth.from(YEAR_MONTH.parse(dateString)).atDay(1);
				LocalDateTime startOfDayOnDate = date.atStartOfDay();
				long begin = startOfDayOnDate.toInstant(ZoneOffset.UTC).toEpochMilli();
				long end = startOfDayOnDate.plusMonths(1).toInstant(ZoneOffset.UTC).toEpochMilli() - 1;
				return new DateBounds(begin, end);
			}
			catch (DateTimeParseException ignored) {

			}
			try {
				date = Year.from(YEAR_ONLY.parse(dateString)).atDay(1);
				LocalDateTime startOfDayOnDate = date.atStartOfDay();
				long begin = startOfDayOnDate.toInstant(ZoneOffset.UTC).toEpochMilli();
				long end = startOfDayOnDate.plusYears(1).toInstant(ZoneOffset.UTC).toEpochMilli() - 1;
				return new DateBounds(begin, end);
			}
			catch (DateTimeParseException ignored) {
				return null;
			}

		}

	}

}
