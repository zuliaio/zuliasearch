package io.zulia.data.source.spreadsheet;

import io.zulia.util.BooleanUtil;
import io.zulia.util.ZuliaDateUtil;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
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
	 * ISO date time as {@link #isoDateParser(ZoneId)} reads it, or a plain ISO date such as 2024-05-01 that is read as the start of
	 * that day in the given zone.
	 *
	 */
	public static Function<String, Date> flexibleIsoDateParser(ZoneId zoneId) {
		// mirrors how the JDK builds ISO_DATE_TIME, with the time section optional. STRICT keeps SMART from turning Feb 30 into Feb 29
		DateTimeFormatter formatter = new DateTimeFormatterBuilder().parseCaseInsensitive().append(DateTimeFormatter.ISO_LOCAL_DATE).optionalStart()
				.appendLiteral('T').append(DateTimeFormatter.ISO_LOCAL_TIME).optionalEnd().optionalStart().appendOffsetId().optionalEnd().optionalStart()
				.appendLiteral('[').parseCaseSensitive().appendZoneRegionId().appendLiteral(']').optionalEnd().parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
				.toFormatter().withResolverStyle(ResolverStyle.STRICT).withZone(zoneId);
		return (s) -> Date.from(Instant.from(formatter.parse(s)));
	}

	/**
	 * ISO date time with the offset and zone id, for example 2024-12-18T08:00:00Z[Etc/UTC], which {@link #isoDateParser(ZoneId)} reads.
	 */
	public static Function<Date, String> isoDateFormatter(ZoneId zoneId) {
		DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME.withZone(zoneId);
		return (date) -> formatter.format(date.toInstant());
	}

	/**
	 * Handles every date text form a Zulia date field accepts, listed in {@link ZuliaDateUtil#SUPPORTED_DATE_STRING_FORMATS}. A value
	 * without an offset is read as UTC, and a date, year month or year is read as the start of that period in UTC. Use this
	 * where the values are indexed into Zulia, so a cell is read the way the index would read it. Text that ends in a bracketed
	 * zone id, which is what {@link #isoDateFormatter(ZoneId)} writes and no Zulia form can end in, is read as ISO date time
	 * instead, so files written with the default formatter still read.
	 * A day past the end of its month is moved to the last day, as the index does, where {@link #flexibleIsoDateParser(ZoneId)} rejects it.
	 * The year always comes first. A month first or day first date such as 5/1/2024 is refused on purpose, since both readings
	 * are valid dates and a guess would store the wrong one without an error.
	 */
	public static Function<String, Date> zuliaDateParser() {
		Function<String, Date> isoDateParser = isoDateParser(ZoneOffset.UTC);
		return (s) -> {
			if (s.endsWith("]")) {
				return isoDateParser.apply(s);
			}
			try {
				return ZuliaDateUtil.convertToDate(s, "cell");
			}
			catch (IllegalArgumentException e) {
				// the other date parsers throw DateTimeParseException, so callers keep one failure type
				throw new DateTimeParseException(e.getMessage(), s, 0, e);
			}
		};
	}

	/**
	 * ISO instant in UTC, for example 2024-12-18T08:00:00Z, which {@link #zuliaDateParser()} reads.
	 */
	public static Function<Date, String> zuliaDateFormatter() {
		return (date) -> DateTimeFormatter.ISO_INSTANT.format(date.toInstant());
	}

	/**
	 * The default boolean parser with {@link #zuliaDateParser()} and {@link #zuliaDateFormatter()}, read and written.
	 */
	public static CellParsers zuliaDates() {
		return new CellParsers(BooleanUtil::parseBoolean, zuliaDateParser(), zuliaDateFormatter());
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
