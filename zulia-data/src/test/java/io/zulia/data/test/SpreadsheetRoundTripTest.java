package io.zulia.data.test;

import io.zulia.data.input.SingleUseDataInputStream;
import io.zulia.data.output.SingleUseDataOutputStream;
import io.zulia.data.source.spreadsheet.SpreadsheetRecord;
import io.zulia.data.source.spreadsheet.SpreadsheetSource;
import io.zulia.data.source.spreadsheet.SpreadsheetSourceFactory;
import io.zulia.data.target.spreadsheet.SpreadsheetTargetFactory;
import org.apache.poi.util.LocaleUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
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

	private static byte[] write(String fileName) throws IOException {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		try (var target = SpreadsheetTargetFactory.fromStreamWithHeaders(SingleUseDataOutputStream.from(bytes, fileName), HEADERS)) {
			target.writeRow(DATE, true, 42L, 1.5d, DATES, FLAGS, COUNTS);
		}
		return bytes.toByteArray();
	}
}
