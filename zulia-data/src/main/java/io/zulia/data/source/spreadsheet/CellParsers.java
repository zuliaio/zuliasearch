package io.zulia.data.source.spreadsheet;

import io.zulia.util.BooleanUtil;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Objects;
import java.util.function.Function;

/**
 * How cell text is read as a Boolean or a Date, and how a Date is written back as text.
 * One instance is shared by whole-cell reads and by {@link DefaultDelimitedListHandler}, so a value is read the same way
 * whether it fills a cell or is one element of a delimited list, and a Date list element is written in the form the
 * date parser reads.
 *
 * @param booleanParser parses one trimmed element or cell. Unrecognised text may map to null
 * @param dateParser    parses one trimmed element or cell. Unparseable text should throw
 * @param dateFormatter writes a Date as text the date parser reads back
 */
public record CellParsers(Function<String, Boolean> booleanParser, Function<String, Date> dateParser, Function<Date, String> dateFormatter) {

	private static final CellParsers DEFAULTS = new CellParsers(BooleanUtil::parseBoolean, isoDateParser(ZoneId.systemDefault()),
			isoDateFormatter(ZoneId.systemDefault()));

	public CellParsers {
		Objects.requireNonNull(booleanParser, "booleanParser");
		Objects.requireNonNull(dateParser, "dateParser");
		Objects.requireNonNull(dateFormatter, "dateFormatter");
	}

	/**
	 * Keeps the default ISO date formatter, which reads back through the default date parser.
	 */
	public CellParsers(Function<String, Boolean> booleanParser, Function<String, Date> dateParser) {
		this(booleanParser, dateParser, DEFAULTS.dateFormatter());
	}

	/**
	 * {@link BooleanUtil#parseBoolean(String)} for booleans and ISO date time in the system default zone for dates, read and written.
	 */
	public static CellParsers defaults() {
		return DEFAULTS;
	}

	/**
	 * ISO date time with an optional offset or zone id, for example 2024-12-18T08:00:00Z[Etc/UTC] as written by the date target handlers.
	 * A value without an offset or zone is read in the given zone.
	 */
	public static Function<String, Date> isoDateParser(ZoneId zoneId) {
		DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME.withZone(zoneId);
		return (s) -> Date.from(Instant.from(formatter.parse(s)));
	}

	/**
	 * ISO date time with the offset and zone id, for example 2024-12-18T08:00:00Z[Etc/UTC], which {@link #isoDateParser(ZoneId)} reads.
	 */
	public static Function<Date, String> isoDateFormatter(ZoneId zoneId) {
		DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME.withZone(zoneId);
		return (date) -> formatter.format(date.toInstant());
	}

	public CellParsers withBooleanParser(Function<String, Boolean> booleanParser) {
		return new CellParsers(booleanParser, dateParser, dateFormatter);
	}

	public CellParsers withDateParser(Function<String, Date> dateParser) {
		return new CellParsers(booleanParser, dateParser, dateFormatter);
	}

	public CellParsers withDateFormatter(Function<Date, String> dateFormatter) {
		return new CellParsers(booleanParser, dateParser, dateFormatter);
	}
}
