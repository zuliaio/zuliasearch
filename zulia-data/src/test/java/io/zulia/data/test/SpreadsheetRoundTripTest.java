package io.zulia.data.test;

import io.zulia.data.input.SingleUseDataInputStream;
import io.zulia.data.output.SingleUseDataOutputStream;
import io.zulia.data.source.spreadsheet.CellParsers;
import io.zulia.data.source.spreadsheet.SpreadsheetRecord;
import io.zulia.data.source.spreadsheet.SpreadsheetSource;
import io.zulia.data.source.spreadsheet.SpreadsheetSourceFactory;
import io.zulia.data.source.spreadsheet.SpreadsheetSourceFactory.HeaderOptions;
import io.zulia.data.target.spreadsheet.SpreadsheetTargetConfig;
import io.zulia.data.target.spreadsheet.SpreadsheetTargetFactory;
import org.bson.Document;
import org.apache.poi.util.LocaleUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * What a target writes with its defaults, the matching source reads back with its defaults: whole cells and delimited lists of
 * dates, booleans and numbers, with null list elements kept as placeholders.
 */
public class SpreadsheetRoundTripTest {

	private static final List<String> HEADERS = List.of("date", "flag", "count", "amount", "dates", "flags", "counts");
	private static final Date DATE = Date.from(Instant.parse("2024-12-18T08:00:00Z"));
	private static final List<Date> DATES = List.of(DATE, Date.from(Instant.parse("2025-01-02T00:00:00Z")));
	private static final List<Boolean> FLAGS = List.of(true, false);
	private static final List<Long> COUNTS = Arrays.asList(1L, null, 3L);

	@ParameterizedTest
	@ValueSource(strings = { "test.csv", "test.tsv", "test.xlsx" })
	void targetDefaultsReadBackThroughSourceDefaults(String fileName) throws IOException {
		byte[] bytes = write(fileName);

		SingleUseDataInputStream in = SingleUseDataInputStream.from(new ByteArrayInputStream(bytes), fileName);
		try (SpreadsheetSource<?> source = SpreadsheetSourceFactory.fromStreamWithHeaders(in)) {
			SpreadsheetRecord row = source.iterator().next();
			Assertions.assertEquals(DATE, row.getDate("date"));
			Assertions.assertEquals(Boolean.TRUE, row.getBoolean("flag"));
			Assertions.assertEquals(42L, row.getLong("count"));
			Assertions.assertEquals(1.5d, row.getDouble("amount"));
			Assertions.assertEquals(DATES, row.getList("dates", Date.class));
			Assertions.assertEquals(FLAGS, row.getList("flags", Boolean.class));
			Assertions.assertEquals(COUNTS, row.getList("counts", Long.class));
		}
	}

	/**
	 * The Excel writer and reader both use POI's thread local time zone. Whatever the calling thread had set, a typed date
	 * must come back unchanged.
	 */
	@ParameterizedTest
	@ValueSource(strings = { "America/New_York", "Asia/Tokyo" })
	void excelTypedDatesSurviveTheCallingThreadTimeZone(String zone) throws IOException {
		TimeZone previous = LocaleUtil.getUserTimeZone();
		try {
			LocaleUtil.setUserTimeZone(TimeZone.getTimeZone(zone));
			byte[] bytes = write("test.xlsx");
			LocaleUtil.setUserTimeZone(TimeZone.getTimeZone(zone));
			SingleUseDataInputStream in = SingleUseDataInputStream.from(new ByteArrayInputStream(bytes), "test.xlsx");
			try (SpreadsheetSource<?> source = SpreadsheetSourceFactory.fromStreamWithHeaders(in)) {
				Assertions.assertEquals(DATE, source.iterator().next().getDate("date"));
			}
		}
		finally {
			LocaleUtil.setUserTimeZone(previous);
		}
	}

	/**
	 * A LocalDate is a calendar day with no zone. Excel stores it as a typed date cell and the delimited targets write
	 * the ISO local date, so every format reads it back as the start of that day in UTC. The default source date parser
	 * needs a time, so the plain date is read with the flexible parser. ExcelTargetTest checks the cell is typed.
	 */
	@ParameterizedTest
	@ValueSource(strings = { "test.csv", "test.tsv", "test.xlsx" })
	void localDateReadsBackAsStartOfDayUtc(String fileName) throws IOException {
		LocalDate day = LocalDate.of(2024, 5, 1);
		Date expected = Date.from(day.atStartOfDay(ZoneOffset.UTC).toInstant());

		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		try (var target = SpreadsheetTargetFactory.fromStreamWithHeaders(SingleUseDataOutputStream.from(bytes, fileName), List.of("day"))) {
			target.writeRow(day);
		}

		SingleUseDataInputStream in = SingleUseDataInputStream.from(new ByteArrayInputStream(bytes.toByteArray()), fileName);
		try (SpreadsheetSource<?> source = SpreadsheetSourceFactory.fromStream(in, HeaderOptions.STANDARD,
				config -> config.withDateParser(CellParsers.flexibleIsoDateParser(ZoneOffset.UTC)))) {
			Assertions.assertEquals(expected, source.iterator().next().getDate("day"));
		}
	}

	/**
	 * An Instant, ZonedDateTime or OffsetDateTime is the same point in time as a Date, so each is written through the
	 * date handler and reads back as that Date whatever zone or offset it carried.
	 */
	@ParameterizedTest
	@ValueSource(strings = { "test.csv", "test.tsv", "test.xlsx" })
	void pointInTimeTypesReadBackAsTheSameDate(String fileName) throws IOException {
		Instant instant = DATE.toInstant();
		ZonedDateTime zoned = instant.atZone(ZoneId.of("Asia/Tokyo"));
		OffsetDateTime offset = instant.atOffset(ZoneOffset.ofHours(-5));
		List<Instant> instants = List.of(instant, instant.plusSeconds(60));

		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		try (var target = SpreadsheetTargetFactory.fromStreamWithHeaders(SingleUseDataOutputStream.from(bytes, fileName),
				List.of("instant", "zoned", "offset", "instants"))) {
			target.writeRow(instant, zoned, offset, instants);
		}

		SingleUseDataInputStream in = SingleUseDataInputStream.from(new ByteArrayInputStream(bytes.toByteArray()), fileName);
		try (SpreadsheetSource<?> source = SpreadsheetSourceFactory.fromStreamWithHeaders(in)) {
			SpreadsheetRecord row = source.iterator().next();
			Assertions.assertEquals(DATE, row.getDate("instant"));
			Assertions.assertEquals(DATE, row.getDate("zoned"));
			Assertions.assertEquals(DATE, row.getDate("offset"));
			Assertions.assertEquals(instant, row.getInstant("instant"));
			Assertions.assertEquals(instant, row.getInstant(2));
			Assertions.assertEquals(instants, row.getList("instants", Instant.class));
		}
	}

	/**
	 * A LocalDate, alone or in a list, reads back as a LocalDate through the source defaults in every format.
	 */
	@ParameterizedTest
	@ValueSource(strings = { "test.csv", "test.tsv", "test.xlsx" })
	void localDateReadsBackAsLocalDate(String fileName) throws IOException {
		LocalDate day = LocalDate.of(2024, 5, 1);
		List<LocalDate> days = Arrays.asList(day, null, LocalDate.of(2024, 2, 29));

		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		try (var target = SpreadsheetTargetFactory.fromStreamWithHeaders(SingleUseDataOutputStream.from(bytes, fileName), List.of("day", "days"))) {
			target.writeRow(day, days);
		}

		SingleUseDataInputStream in = SingleUseDataInputStream.from(new ByteArrayInputStream(bytes.toByteArray()), fileName);
		try (SpreadsheetSource<?> source = SpreadsheetSourceFactory.fromStreamWithHeaders(in)) {
			SpreadsheetRecord row = source.iterator().next();
			Assertions.assertEquals(day, row.getLocalDate("day"));
			Assertions.assertEquals(day, row.getLocalDate(0));
			Assertions.assertEquals(days, row.getList("days", LocalDate.class));
		}
	}

	/**
	 * A LocalDateTime, alone or in a list, reads back as a LocalDateTime through the source defaults in every format.
	 */
	@ParameterizedTest
	@ValueSource(strings = { "test.csv", "test.tsv", "test.xlsx" })
	void localDateTimeReadsBackAsLocalDateTime(String fileName) throws IOException {
		LocalDateTime at = LocalDateTime.of(2024, 5, 1, 13, 45, 30);
		List<LocalDateTime> ats = Arrays.asList(at, null, at.plusHours(1));

		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		try (var target = SpreadsheetTargetFactory.fromStreamWithHeaders(SingleUseDataOutputStream.from(bytes, fileName), List.of("at", "ats"))) {
			target.writeRow(at, ats);
		}

		SingleUseDataInputStream in = SingleUseDataInputStream.from(new ByteArrayInputStream(bytes.toByteArray()), fileName);
		try (SpreadsheetSource<?> source = SpreadsheetSourceFactory.fromStreamWithHeaders(in)) {
			SpreadsheetRecord row = source.iterator().next();
			Assertions.assertEquals(at, row.getLocalDateTime("at"));
			Assertions.assertEquals(at, row.getLocalDateTime(0));
			Assertions.assertEquals(ats, row.getList("ats", LocalDateTime.class));
		}
	}

	/**
	 * An array, object or primitive, is written like a Collection and reads back as a list.
	 */
	@ParameterizedTest
	@ValueSource(strings = { "test.csv", "test.tsv", "test.xlsx" })
	void arraysWriteAsLists(String fileName) throws IOException {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		try (var target = SpreadsheetTargetFactory.fromStreamWithHeaders(SingleUseDataOutputStream.from(bytes, fileName),
				List.of("strings", "ints", "doubles", "flags"))) {
			target.writeRow(new String[] { "a", "b" }, new int[] { 1, 2, 3 }, new double[] { 1.5, 2.5 }, new boolean[] { true, false });
		}

		SingleUseDataInputStream in = SingleUseDataInputStream.from(new ByteArrayInputStream(bytes.toByteArray()), fileName);
		try (SpreadsheetSource<?> source = SpreadsheetSourceFactory.fromStreamWithHeaders(in)) {
			SpreadsheetRecord row = source.iterator().next();
			Assertions.assertEquals(List.of("a", "b"), row.getList("strings", String.class));
			Assertions.assertEquals(List.of(1, 2, 3), row.getList("ints", Integer.class));
			Assertions.assertEquals(List.of(1.5, 2.5), row.getList("doubles", Double.class));
			Assertions.assertEquals(List.of(true, false), row.getList("flags", Boolean.class));
		}
	}

	/**
	 * A Map, including a BSON Document, is written as JSON text that Document.parse reads back with its values intact.
	 */
	@ParameterizedTest
	@ValueSource(strings = { "test.csv", "test.tsv", "test.xlsx" })
	void mapsWriteAsJson(String fileName) throws IOException {
		Document document = new Document("name", "x").append("count", 2).append("when", DATE).append("nested", new Document("k", "v"));
		Map<String, Object> map = new LinkedHashMap<>();
		map.put("a", 1);
		map.put("b", List.of("x", "y"));

		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		try (var target = SpreadsheetTargetFactory.fromStreamWithHeaders(SingleUseDataOutputStream.from(bytes, fileName), List.of("document", "map"))) {
			target.writeRow(document, map);
		}

		SingleUseDataInputStream in = SingleUseDataInputStream.from(new ByteArrayInputStream(bytes.toByteArray()), fileName);
		try (SpreadsheetSource<?> source = SpreadsheetSourceFactory.fromStreamWithHeaders(in)) {
			SpreadsheetRecord row = source.iterator().next();
			Assertions.assertEquals(document, Document.parse(row.getString("document")));
			Assertions.assertEquals(new Document(map), Document.parse(row.getString("map")));
		}
	}

	/**
	 * A custom date handler written as a lambda only sees a Date. It must still be used for a LocalDate, which arrives
	 * as the start of that day in UTC.
	 */
	@ParameterizedTest
	@ValueSource(strings = { "test.csv", "test.tsv", "test.xlsx" })
	void lambdaDateHandlerCoversLocalDate(String fileName) throws IOException {
		LocalDate day = LocalDate.of(2024, 5, 1);
		List<Date> seen = new ArrayList<>();

		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		try (var target = SpreadsheetTargetFactory.fromStreamWithHeaders(SingleUseDataOutputStream.from(bytes, fileName), List.of("day"),
				config -> recordDatesAsCustomText(config, seen))) {
			target.writeRow(day);
		}

		Assertions.assertEquals(List.of(Date.from(day.atStartOfDay(ZoneOffset.UTC).toInstant())), seen);
		SingleUseDataInputStream in = SingleUseDataInputStream.from(new ByteArrayInputStream(bytes.toByteArray()), fileName);
		try (SpreadsheetSource<?> source = SpreadsheetSourceFactory.fromStreamWithHeaders(in)) {
			Assertions.assertEquals("custom", source.iterator().next().getString("day"));
		}
	}

	private static <T> void recordDatesAsCustomText(SpreadsheetTargetConfig<T, ?> config, List<Date> seen) {
		config.withDateTypeHandler((reference, value) -> {
			seen.add(value);
			config.getStringTypeHandler().writeType(reference, "custom");
		});
	}

	private static byte[] write(String fileName) throws IOException {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		try (var target = SpreadsheetTargetFactory.fromStreamWithHeaders(SingleUseDataOutputStream.from(bytes, fileName), HEADERS)) {
			target.writeRow(DATE, true, 42L, 1.5d, DATES, FLAGS, COUNTS);
		}
		return bytes.toByteArray();
	}
}
