package io.zulia.data.test;

import io.zulia.data.source.spreadsheet.CellParsers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.function.Function;

/**
 * {@link CellParsers#flexibleIsoDateParser(ZoneId)} reads everything {@link CellParsers#isoDateParser(ZoneId)} reads plus a
 * plain ISO date, and still throws on text that is neither, naming the field that is wrong.
 */
public class FlexibleDateParserTest {

	private static final ZoneId ZONE = ZoneId.of("America/New_York");
	private static final Function<String, Date> PARSER = CellParsers.flexibleIsoDateParser(ZONE);

	@Test
	void readsPlainIsoDateAsStartOfDay() {
		Assertions.assertEquals(Date.from(LocalDate.of(2024, 5, 1).atStartOfDay(ZONE).toInstant()), PARSER.apply("2024-05-01"));
	}

	@Test
	void readsIsoDateTimeWithZone() {
		Assertions.assertEquals(Date.from(Instant.parse("2024-12-18T08:00:00Z")), PARSER.apply("2024-12-18T08:00:00Z[Etc/UTC]"));
	}

	@Test
	void readsIsoDateTimeWithOffset() {
		Assertions.assertEquals(Date.from(Instant.parse("2024-12-18T07:00:00Z")), PARSER.apply("2024-12-18T08:00:00+01:00"));
	}

	@Test
	void readsIsoDateTimeWithoutOffsetInTheGivenZone() {
		Assertions.assertEquals(Date.from(LocalDate.of(2024, 12, 18).atStartOfDay(ZONE).plusHours(8).toInstant()), PARSER.apply("2024-12-18T08:00:00"));
	}

	@Test
	void readsLowerCaseSeparatorsLikeTheIsoDateTimeParser() {
		Assertions.assertEquals(Date.from(Instant.parse("2024-05-01T08:00:00Z")), PARSER.apply("2024-05-01t08:00:00z"));
	}

	@Test
	void namesTheInvalidHour() {
		DateTimeParseException e = Assertions.assertThrows(DateTimeParseException.class, () -> PARSER.apply("2024-05-01T25:00:00"));
		Assertions.assertTrue(e.getMessage().contains("HourOfDay"), e.getMessage());
	}

	@Test
	void rejectsAnInvalidDayInsteadOfClampingIt() {
		DateTimeParseException leapDay = Assertions.assertThrows(DateTimeParseException.class, () -> PARSER.apply("2023-02-29"));
		Assertions.assertTrue(leapDay.getMessage().contains("not a leap year"), leapDay.getMessage());
		Assertions.assertThrows(DateTimeParseException.class, () -> PARSER.apply("2024-02-30"));
	}

	@Test
	void throwsOnTextThatIsNeither() {
		Assertions.assertThrows(DateTimeParseException.class, () -> PARSER.apply("N/A"));
		Assertions.assertThrows(DateTimeParseException.class, () -> PARSER.apply("05/01/2024"));
		Assertions.assertThrows(DateTimeParseException.class, () -> PARSER.apply("20240501"));
	}
}
