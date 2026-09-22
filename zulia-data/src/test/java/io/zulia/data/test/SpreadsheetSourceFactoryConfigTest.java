package io.zulia.data.test;

import io.zulia.data.input.SingleUseDataInputStream;
import io.zulia.data.output.SingleUseDataOutputStream;
import io.zulia.data.source.spreadsheet.CellParsers;
import io.zulia.data.source.spreadsheet.SpreadsheetRecord;
import io.zulia.data.source.spreadsheet.SpreadsheetSource;
import io.zulia.data.source.spreadsheet.SpreadsheetSourceConfig;
import io.zulia.data.source.spreadsheet.SpreadsheetSourceFactory;
import io.zulia.data.source.spreadsheet.SpreadsheetSourceFactory.HeaderOptions;
import io.zulia.data.target.spreadsheet.SpreadsheetTargetFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

/**
 * The factory applies a caller's configurer to whichever type-specific config the stream selects, so a source can be configured
 * without knowing whether it is delimited or Excel.
 */
public class SpreadsheetSourceFactoryConfigTest {

	private static final ZoneId ZONE = ZoneId.of("America/New_York");
	private static final Date MAY_FIRST = Date.from(LocalDate.of(2024, 5, 1).atStartOfDay(ZONE).toInstant());
	private static final CellParsers PLAIN_DATES = CellParsers.defaults().withDateParser(CellParsers.flexibleIsoDateParser(ZONE));

	@ParameterizedTest
	@ValueSource(strings = { "test.csv", "test.tsv", "test.xlsx" })
	void customDateParserReadsPlainDates(String fileName) throws IOException {
		try (SpreadsheetSource<?> source = open(fileName, config -> config.withParsers(PLAIN_DATES))) {
			Assertions.assertEquals(MAY_FIRST, source.iterator().next().getDate("date"));
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "test.csv", "test.tsv", "test.xlsx" })
	void defaultParserStillRejectsPlainDates(String fileName) throws IOException {
		try (SpreadsheetSource<?> source = open(fileName, null)) {
			Assertions.assertThrows(DateTimeParseException.class, () -> source.iterator().next().getDate("date"));
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "test.csv", "test.tsv", "test.xlsx" })
	void listDelimiterAppliesToEveryType(String fileName) throws IOException {
		try (SpreadsheetSource<?> source = open(fileName, config -> config.withListDelimiter('|'))) {
			SpreadsheetRecord record = source.iterator().next();
			Assertions.assertEquals(List.of("a", "b"), record.getList(1, String.class));
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "test.csv", "test.tsv", "test.xlsx" })
	void configurerRunsAfterHeaderOptions(String fileName) throws IOException {
		try (SpreadsheetSource<?> source = open(fileName, SpreadsheetSourceConfig::withoutHeaders)) {
			Assertions.assertEquals("date", source.iterator().next().getString(0));
		}
	}

	private static SpreadsheetSource<?> open(String fileName, Consumer<SpreadsheetSourceConfig> configurer) throws IOException {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		try (var target = SpreadsheetTargetFactory.fromStreamWithHeaders(SingleUseDataOutputStream.from(bytes, fileName), List.of("date", "tags"))) {
			target.writeRow("2024-05-01", "a|b");
		}
		SingleUseDataInputStream in = SingleUseDataInputStream.from(bytes.toByteArray(), fileName);
		return SpreadsheetSourceFactory.fromStream(in, HeaderOptions.STANDARD, configurer);
	}
}
