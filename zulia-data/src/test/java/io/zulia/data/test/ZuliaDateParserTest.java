package io.zulia.data.test;

import io.zulia.data.source.spreadsheet.CellParsers;
import io.zulia.util.ZuliaDateUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.function.Function;

/**
 * {@link CellParsers#zuliaDateParser()} reads the date text a Zulia date field accepts, always in UTC, and
 * {@link CellParsers#zuliaDateFormatter()} writes text it reads back.
 */
public class ZuliaDateParserTest {

	private static final Function<String, Date> PARSER = CellParsers.zuliaDateParser();

	private static Date utc(String instant) {
		return Date.from(Instant.parse(instant));
	}

	@Test
	void readsTimestampWithOffset() {
		Assertions.assertEquals(utc("2024-12-18T07:00:00Z"), PARSER.apply("2024-12-18T08:00:00+01:00"));
		Assertions.assertEquals(utc("2024-12-18T08:00:00Z"), PARSER.apply("2024-12-18T08:00:00Z"));
	}

	@Test
	void readsTimestampWithoutOffsetAsUtc() {
		Assertions.assertEquals(utc("2024-12-18T08:00:00Z"), PARSER.apply("2024-12-18T08:00:00"));
		Assertions.assertEquals(utc("2024-12-18T08:00:00Z"), PARSER.apply("2024-12-18T08:00"));
	}

	@Test
	void readsDateAsStartOfDayInUtc() {
		Assertions.assertEquals(utc("2024-05-01T00:00:00Z"), PARSER.apply("2024-05-01"));
		Assertions.assertEquals(utc("2024-05-01T00:00:00Z"), PARSER.apply("2024-5-1"));
	}

	@Test
	void readsSlashesInPlaceOfDashes() {
		Assertions.assertEquals(utc("2024-05-01T00:00:00Z"), PARSER.apply("2024/05/01"));
	}

	@Test
	void readsYearMonthAndYearAsStartOfPeriod() {
		Assertions.assertEquals(utc("2024-05-01T00:00:00Z"), PARSER.apply("2024-05"));
		Assertions.assertEquals(utc("2024-01-01T00:00:00Z"), PARSER.apply("2024"));
	}

	@Test
	void rejectsOtherTextWithTheSupportedFormats() {
		DateTimeParseException e = Assertions.assertThrows(DateTimeParseException.class, () -> PARSER.apply("5/1/2024"));
		Assertions.assertEquals("5/1/2024", e.getParsedString());
		Assertions.assertTrue(e.getMessage().contains(ZuliaDateUtil.SUPPORTED_DATE_STRING_FORMATS), e.getMessage());
		Assertions.assertThrows(DateTimeParseException.class, () -> PARSER.apply("not a date"));
	}

	@Test
	void readsTheBracketedZoneIdTheDefaultFormatterWrites() {
		Date date = utc("2024-12-18T08:00:00Z");
		Assertions.assertEquals(date, PARSER.apply(CellParsers.defaults().dateFormatter().apply(date)));
		Assertions.assertEquals(date, PARSER.apply("2024-12-18T08:00:00Z[Etc/UTC]"));
		Assertions.assertEquals(date, PARSER.apply("2024-12-18T03:00:00-05:00[America/New_York]"));
	}

	@Test
	void bracketedTextThatIsNotIsoDateTimeIsRejected() {
		Assertions.assertThrows(DateTimeParseException.class, () -> PARSER.apply("2024-12-18[Etc/UTC]"));
		Assertions.assertThrows(DateTimeParseException.class, () -> PARSER.apply("5/1/2024T08:00:00Z[Etc/UTC]"));
		Assertions.assertThrows(DateTimeParseException.class, () -> PARSER.apply("2024-12-18T08:00:00Z[Not/AZone]"));
	}

	@Test
	void formatterWritesWhatTheParserReads() {
		CellParsers parsers = CellParsers.zuliaDates();
		for (String instant : new String[] { "2024-12-18T08:00:00Z", "2024-12-18T08:00:00.123Z", "1969-07-20T20:17:40Z" }) {
			Date date = utc(instant);
			String written = parsers.dateFormatter().apply(date);
			Assertions.assertEquals(date, parsers.dateParser().apply(written), written);
		}
		Assertions.assertEquals("2024-12-18T08:00:00Z", parsers.dateFormatter().apply(utc("2024-12-18T08:00:00Z")));
	}
}
