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
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;

public class ZuliaDateUtil {

	// zone offset is optional and when omitted, the timestamp is assumed to be UTC (OFFSET_SECONDS defaults to 0 only when
	// not parsed from the text, so an explicit offset such as Z or +05:30 is honored)
	public static final DateTimeFormatter YEAR_MONTH_DAY_TIME = new DateTimeFormatterBuilder().appendValue(ChronoField.YEAR, 4).appendLiteral('-')
			.appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NEVER).appendLiteral('-').appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NEVER)
			.appendLiteral('T').append(ISO_LOCAL_TIME).optionalStart().appendOffsetId().optionalEnd().parseDefaulting(ChronoField.OFFSET_SECONDS, 0)
			.toFormatter();

	public static final DateTimeFormatter YEAR_MONTH_DAY = new DateTimeFormatterBuilder().appendValue(ChronoField.YEAR, 4).appendLiteral('-')
			.appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NEVER).appendLiteral('-').appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NEVER)
			.toFormatter();

	public static final DateTimeFormatter YEAR_MONTH = new DateTimeFormatterBuilder().appendValue(ChronoField.YEAR, 4).appendLiteral('-')
			.appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NEVER).toFormatter();

	public static final DateTimeFormatter YEAR_ONLY = new DateTimeFormatterBuilder().appendValue(ChronoField.YEAR, 4).toFormatter();

	public record DateFormatSpec(String label, String pattern, String example) {
		public String describe() {
			return label + ": " + pattern + " (e.g. " + example + ")";
		}
	}

	/**
	 * Every date String layout accepted by {@link #getParseDate(String)} / {@link #parseToEpochMilli(String)} /
	 * {@link #convertToDate(Object, String)}. Add a new format here when a new DateTimeFormatter is added / parsing is changed
	 */
	public static final List<DateFormatSpec> SUPPORTED_DATE_FORMATS = List.of(
			new DateFormatSpec("timestamp with optional zone offset (assumed UTC when omitted)", "yyyy-MM-dd'T'HH:mm[:ss[.SSS]][XXX]", "2024-06-17T16:10:00Z"),
			new DateFormatSpec("date", "yyyy-MM-dd", "2024-06-17"), new DateFormatSpec("year-month", "yyyy-MM", "2024-06"),
			new DateFormatSpec("year", "yyyy", "2024"));

	/**
	 * Human-readable description of every supported date String layout, derived from {@link #SUPPORTED_DATE_FORMATS} so
	 * indexing, sorting, faceting, and querying all report the same formats.
	 */
	public static final String SUPPORTED_DATE_STRING_FORMATS =
			SUPPORTED_DATE_FORMATS.stream().map(DateFormatSpec::describe).collect(Collectors.joining("; ")) + "; '/' may be used in place of '-'";

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

	/**
	 * Parses a date String to a single epoch milli suitable for indexing, sorting, or faceting a single value.
	 * For partial dates (year, year-month, year-month-day) the start of the period (UTC) is used, matching the lower
	 * bound used on the query side.
	 *
	 * @param dateString a date in one of the {@link #SUPPORTED_DATE_STRING_FORMATS}
	 * @return the epoch milli, or {@code null} if the String cannot be parsed
	 */
	public static Long parseToEpochMilli(String dateString) {
		DateBounds dateBounds = getParseDate(dateString);
		return dateBounds == null ? null : dateBounds.begin();
	}

	/**
	 * Resolves a stored field value to a {@link Date} for indexing, sorting, and faceting a date field. Accepts a
	 * {@link Date} as-is, a {@link Number} treated as epoch milliseconds, or a String in one of the
	 * {@link #SUPPORTED_DATE_STRING_FORMATS}. A {@code null} or blank value is treated as no value and returns
	 * {@code null}.
	 *
	 * @param value        the value from the stored document
	 * @param fieldContext description of the field used in error messages, e.g. {@code "field <publishDate>"}
	 * @return the resolved Date, or {@code null} when there is no usable value
	 * @throws IllegalArgumentException if the value is not a Date, Number, or parseable date String
	 */
	public static Date convertToDate(Object value, String fieldContext) {
		return switch (value) {
			case null -> null;
			case Date date -> date;
			case Number number -> new Date(number.longValue());
			case String dateString -> {
				if (dateString.isBlank()) {
					yield null;
				}
				Long epochMilli = parseToEpochMilli(dateString);
				if (epochMilli == null) {
					throw new IllegalArgumentException("String value <" + dateString + "> for date " + fieldContext + " cannot be parsed. Supported formats: "
							+ SUPPORTED_DATE_STRING_FORMATS);
				}
				yield new Date(epochMilli);
			}
			default -> throw new IllegalArgumentException(
					"Expecting Date, epoch milliseconds Number, or date String for " + fieldContext + " and found <" + value.getClass().getSimpleName() + ">");
		};
	}

}
