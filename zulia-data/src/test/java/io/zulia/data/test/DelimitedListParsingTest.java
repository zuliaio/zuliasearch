package io.zulia.data.test;

import io.zulia.data.input.SingleUseDataInputStream;
import io.zulia.data.source.spreadsheet.CellParsers;
import io.zulia.data.source.spreadsheet.DefaultDelimitedListHandler;
import io.zulia.data.source.spreadsheet.DelimitedListHandler;
import io.zulia.data.source.spreadsheet.csv.CSVSource;
import io.zulia.data.source.spreadsheet.csv.CSVSourceConfig;
import io.zulia.data.source.spreadsheet.excel.DefaultExcelCellHandler;
import io.zulia.data.source.spreadsheet.excel.ExcelRecord;
import io.zulia.data.source.spreadsheet.excel.ExcelSource;
import io.zulia.data.source.spreadsheet.excel.ExcelSourceConfig;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.function.Function;

import static io.zulia.data.test.SpreadsheetTestFiles.csvStream;
import static io.zulia.data.test.SpreadsheetTestFiles.header;
import static io.zulia.data.test.SpreadsheetTestFiles.workbook;

/**
 * Delimited lists inside a cell parse Date and Boolean elements with the same parsers the source config uses for whole cells.
 * The Excel side is covered through real workbooks because Excel cells hold lists as strings while typed cells go through the cell handler.
 */
public class DelimitedListParsingTest {

	private static final String DATE_LIST = "2024-12-18T08:00:00Z[Etc/UTC]; 2024-12-19T00:00:00Z";
	private static final List<Date> EXPECTED_DATES = List.of(Date.from(Instant.parse("2024-12-18T08:00:00Z")), Date.from(Instant.parse("2024-12-19T00:00:00Z")));

	// a parser for yyyyMMdd that is distinguishable from the ISO default
	private static final Function<String, Date> COMPACT_DATE_PARSER = s -> Date.from(
			LocalDate.parse(s, DateTimeFormatter.BASIC_ISO_DATE).atStartOfDay(ZoneOffset.UTC).toInstant());
	private static final Function<String, Boolean> ONE_IS_TRUE = s -> "1".equals(s);

	// ---- Excel through the real source

	@Test
	void excelListsOfDatesAndBooleansParseWithDefaults() throws IOException {
		byte[] xlsx = workbook(sheet -> {
			header(sheet, "dates", "flags", "numbers", "words");
			Row row = sheet.createRow(1);
			row.createCell(0).setCellValue(DATE_LIST);
			row.createCell(1).setCellValue("true;no;1");
			row.createCell(2).setCellValue("1;2;3");
			row.createCell(3).setCellValue("a;;b");
		});

		try (ExcelSource source = ExcelSource.withConfig(ExcelSourceConfig.from(SingleUseDataInputStream.from(xlsx, "test.xlsx")).withHeaders())) {
			ExcelRecord record = source.iterator().next();
			Assertions.assertEquals(EXPECTED_DATES, record.getList("dates", Date.class));
			Assertions.assertEquals(EXPECTED_DATES, record.getList(0, Date.class));
			Assertions.assertEquals(List.of(true, false, true), record.getList("flags", Boolean.class));
			Assertions.assertEquals(List.of(true, false, true), record.getList(1, Boolean.class));
			// the previously supported element types are untouched, including blank string elements
			Assertions.assertEquals(List.of(1L, 2L, 3L), record.getList("numbers", Long.class));
			Assertions.assertEquals(List.of(1, 2, 3), record.getList("numbers", Integer.class));
			Assertions.assertEquals(List.of("a", "", "b"), record.getList("words", String.class));
		}
	}

	@Test
	void excelConfigParsersReachTheListHandler() throws IOException {
		byte[] xlsx = workbook(sheet -> {
			header(sheet, "dates", "flags");
			Row row = sheet.createRow(1);
			row.createCell(0).setCellValue("20241218;20241219");
			row.createCell(1).setCellValue("true;1");
		});

		ExcelSourceConfig config = ExcelSourceConfig.from(SingleUseDataInputStream.from(xlsx, "test.xlsx")).withHeaders();
		config.withDateParser(COMPACT_DATE_PARSER).withBooleanParser(ONE_IS_TRUE);
		Assertions.assertSame(COMPACT_DATE_PARSER, config.getDateParser());
		Assertions.assertSame(ONE_IS_TRUE, config.getBooleanParser());

		try (ExcelSource source = ExcelSource.withConfig(config)) {
			ExcelRecord record = source.iterator().next();
			Assertions.assertEquals(List.of(Date.from(Instant.parse("2024-12-18T00:00:00Z")), Date.from(Instant.parse("2024-12-19T00:00:00Z"))),
					record.getList("dates", Date.class));
			// "true" is not "1" for the custom parser
			Assertions.assertEquals(List.of(false, true), record.getList("flags", Boolean.class));
		}
	}

	@Test
	void excelDelimiterChangeKeepsTheParsersInEitherOrder() throws IOException {
		byte[] xlsx = workbook(sheet -> {
			header(sheet, "dates");
			sheet.createRow(1).createCell(0).setCellValue("20241218|20241219");
		});
		List<Date> expected = List.of(Date.from(Instant.parse("2024-12-18T00:00:00Z")), Date.from(Instant.parse("2024-12-19T00:00:00Z")));

		ExcelSourceConfig parserFirst = ExcelSourceConfig.from(SingleUseDataInputStream.from(xlsx, "test.xlsx")).withHeaders();
		parserFirst.withDateParser(COMPACT_DATE_PARSER).withListDelimiter('|');
		try (ExcelSource source = ExcelSource.withConfig(parserFirst)) {
			Assertions.assertEquals(expected, source.iterator().next().getList("dates", Date.class));
		}

		ExcelSourceConfig delimiterFirst = ExcelSourceConfig.from(SingleUseDataInputStream.from(xlsx, "test.xlsx")).withHeaders();
		delimiterFirst.withListDelimiter('|').withDateParser(COMPACT_DATE_PARSER);
		try (ExcelSource source = ExcelSource.withConfig(delimiterFirst)) {
			Assertions.assertEquals(expected, source.iterator().next().getList("dates", Date.class));
		}
	}

	@Test
	void excelCustomListHandlerIsNotReplacedByAParserChange() throws IOException {
		byte[] xlsx = workbook(sheet -> {
			header(sheet, "anything");
			sheet.createRow(1).createCell(0).setCellValue("ignored");
		});
		DelimitedListHandler fixed = new FixedListHandler();

		ExcelSourceConfig config = ExcelSourceConfig.from(SingleUseDataInputStream.from(xlsx, "test.xlsx")).withHeaders();
		config.withDelimitedListHandler(fixed).withDateParser(COMPACT_DATE_PARSER).withBooleanParser(ONE_IS_TRUE);
		Assertions.assertSame(fixed, config.getDelimitedListHandler());

		try (ExcelSource source = ExcelSource.withConfig(config)) {
			Assertions.assertEquals(List.of("fixed"), source.iterator().next().getList("anything", String.class));
		}

		// a later delimiter change deliberately goes back to the default handler, as it did before
		config.withListDelimiter('|');
		Assertions.assertInstanceOf(DefaultDelimitedListHandler.class, config.getDelimitedListHandler());
	}

	@Test
	void excelTypedCellsStillGoThroughTheCellHandler() throws IOException {
		Date typedDate = Date.from(Instant.parse("2024-12-18T00:00:00Z"));
		byte[] xlsx = workbook(sheet -> {
			header(sheet, "date", "flag");
			Row row = sheet.createRow(1);
			CellStyle dateStyle = sheet.getWorkbook().createCellStyle();
			dateStyle.setDataFormat(sheet.getWorkbook().getCreationHelper().createDataFormat().getFormat("yyyy-mm-dd"));
			row.createCell(0).setCellValue(typedDate);
			row.getCell(0).setCellStyle(dateStyle);
			row.createCell(1).setCellValue(true);
		});

		// a config parser that would fail on anything must not be consulted for typed cells
		ExcelSourceConfig config = ExcelSourceConfig.from(SingleUseDataInputStream.from(xlsx, "test.xlsx")).withHeaders();
		config.withDateParser(s -> {
			throw new IllegalStateException("cell parser used for a typed cell");
		}).withBooleanParser(s -> {
			throw new IllegalStateException("cell parser used for a typed cell");
		});

		try (ExcelSource source = ExcelSource.withConfig(config)) {
			ExcelRecord record = source.iterator().next();
			Assertions.assertEquals(typedDate, record.getDate("date"));
			Assertions.assertEquals(Boolean.TRUE, record.getBoolean("flag"));
		}
	}

	@Test
	void excelTextCellsUseTheConfigParsers() throws IOException {
		byte[] xlsx = workbook(sheet -> {
			header(sheet, "date", "flag");
			Row row = sheet.createRow(1);
			row.createCell(0).setCellValue("20241218");
			row.createCell(1).setCellValue("1");
			Row blank = sheet.createRow(2);
			blank.createCell(0).setCellValue("   ");
			blank.createCell(1).setCellValue("");
		});

		ExcelSourceConfig config = ExcelSourceConfig.from(SingleUseDataInputStream.from(xlsx, "test.xlsx")).withHeaders();
		config.withDateParser(COMPACT_DATE_PARSER).withBooleanParser(ONE_IS_TRUE);

		try (ExcelSource source = ExcelSource.withConfig(config)) {
			var iterator = source.iterator();
			ExcelRecord record = iterator.next();
			Assertions.assertEquals(Date.from(Instant.parse("2024-12-18T00:00:00Z")), record.getDate("date"));
			Assertions.assertEquals(Boolean.TRUE, record.getBoolean("flag"));
			// blank text is null for both, the parsers are not consulted
			ExcelRecord blank = iterator.next();
			Assertions.assertNull(blank.getDate("date"));
			Assertions.assertNull(blank.getBoolean("flag"));
		}
	}

	@Test
	void excelTextDateCellsParseWithTheDefaultParser() throws IOException {
		byte[] xlsx = workbook(sheet -> {
			header(sheet, "date");
			sheet.createRow(1).createCell(0).setCellValue("2024-12-18T08:00:00Z[Etc/UTC]");
			sheet.createRow(2).createCell(0).setCellValue("Wed Dec 18 08:00:00 UTC 2024");
		});

		try (ExcelSource source = ExcelSource.withConfig(ExcelSourceConfig.from(SingleUseDataInputStream.from(xlsx, "test.xlsx")).withHeaders())) {
			var iterator = source.iterator();
			Assertions.assertEquals(Date.from(Instant.parse("2024-12-18T08:00:00Z")), iterator.next().getDate("date"));
			ExcelRecord bad = iterator.next();
			Assertions.assertThrows(DateTimeParseException.class, () -> bad.getDate("date"));
		}
	}

	@Test
	void excelCustomCellHandlerIsNotReplacedByAParserChange() throws IOException {
		byte[] xlsx = workbook(sheet -> {
			header(sheet, "flag");
			sheet.createRow(1).createCell(0).setCellValue("anything");
		});

		ExcelSourceConfig config = ExcelSourceConfig.from(SingleUseDataInputStream.from(xlsx, "test.xlsx")).withHeaders();
		config.withExcelCellHandler(new DefaultExcelCellHandler() {
			@Override
			public Boolean cellToBoolean(Cell cell) {
				return Boolean.TRUE;
			}
		}).withBooleanParser(s -> {
			throw new IllegalStateException("config parser used with an explicit cell handler");
		});

		try (ExcelSource source = ExcelSource.withConfig(config)) {
			Assertions.assertEquals(Boolean.TRUE, source.iterator().next().getBoolean("flag"));
		}
	}

	@Test
	void excelBadDateElementThrowsInsteadOfReturningStrings() throws IOException {
		byte[] xlsx = workbook(sheet -> {
			header(sheet, "dates");
			sheet.createRow(1).createCell(0).setCellValue("2024-12-18T00:00:00Z;Wed Dec 18 08:00:00 UTC 2024");
		});
		try (ExcelSource source = ExcelSource.withDefaults(SingleUseDataInputStream.from(xlsx, "test.xlsx"))) {
			ExcelRecord record = source.iterator().next();
			Assertions.assertThrows(DateTimeParseException.class, () -> record.getList(0, Date.class));
		}
	}

	// ---- CSV mirrors of the config interactions

	@Test
	void csvDelimiterChangeKeepsTheParsersInEitherOrder() throws IOException {
		String csv = "dates\n20241218|20241219\n";
		List<Date> expected = List.of(Date.from(Instant.parse("2024-12-18T00:00:00Z")), Date.from(Instant.parse("2024-12-19T00:00:00Z")));

		CSVSourceConfig parserFirst = CSVSourceConfig.from(csvStream(csv));
		parserFirst.withHeaders().withDateParser(COMPACT_DATE_PARSER).withListDelimiter('|');
		try (CSVSource source = CSVSource.withConfig(parserFirst)) {
			Assertions.assertEquals(expected, source.iterator().next().getList("dates", Date.class));
		}

		CSVSourceConfig delimiterFirst = CSVSourceConfig.from(csvStream(csv));
		delimiterFirst.withHeaders().withListDelimiter('|').withDateParser(COMPACT_DATE_PARSER);
		try (CSVSource source = CSVSource.withConfig(delimiterFirst)) {
			Assertions.assertEquals(expected, source.iterator().next().getList("dates", Date.class));
		}
	}

	@Test
	void csvCustomListHandlerIsNotReplacedByAParserChange() throws IOException {
		DelimitedListHandler fixed = new FixedListHandler();
		CSVSourceConfig config = CSVSourceConfig.from(csvStream("anything\nignored\n"));
		config.withHeaders().withDelimitedListHandler(fixed).withDateParser(COMPACT_DATE_PARSER).withBooleanParser(ONE_IS_TRUE);
		Assertions.assertSame(fixed, config.getDelimitedListHandler());
		try (CSVSource source = CSVSource.withConfig(config)) {
			Assertions.assertEquals(List.of("fixed"), source.iterator().next().getList("anything", String.class));
		}
	}

	@Test
	void csvWholeCellAndListElementParseTheSameWay() throws IOException {
		// the single date column and the list column hold the same text, so getDate and getList must agree
		String csv = "single,list\n2024-12-18T08:00:00Z[Etc/UTC],2024-12-18T08:00:00Z[Etc/UTC]\n";
		CSVSourceConfig config = CSVSourceConfig.from(csvStream(csv));
		config.withHeaders();
		try (CSVSource source = CSVSource.withConfig(config)) {
			var record = source.iterator().next();
			Assertions.assertEquals(List.of(record.getDate("single")), record.getList("list", Date.class));
		}
	}

	// ---- handler on its own

	@Test
	void handlerKeepsBlankElementsAsNullPlaceholders() {
		DefaultDelimitedListHandler handler = new DefaultDelimitedListHandler(';');
		Assertions.assertEquals(Arrays.asList(null, true, null), handler.cellValueToList(Boolean.class, " ; true ; "));
		Assertions.assertEquals(Arrays.asList(null, null), handler.cellValueToList(Date.class, " ; "));
		Assertions.assertEquals(Arrays.asList(1L, null, 2L), handler.cellValueToList(Long.class, "1;;2"));
		Assertions.assertEquals(List.of(1, 2), handler.cellValueToList(Integer.class, " 1 ; 2 "));
		Assertions.assertEquals(Arrays.asList(null, 1.5d), handler.cellValueToList(Double.class, "; 1.5"));
		// a blank whole cell is an empty list, not a single placeholder
		Assertions.assertEquals(List.of(), handler.cellValueToList(Long.class, ""));
		Assertions.assertEquals(List.of(), handler.cellValueToList(Date.class, "   "));
		// string lists are untouched
		Assertions.assertEquals(List.of("", "a", ""), handler.cellValueToList(String.class, ";a;"));
		Assertions.assertThrows(NumberFormatException.class, () -> handler.cellValueToList(Long.class, "1;x;2"));
		// the writer turns the placeholders back into blanks
		Assertions.assertEquals("1;;2", handler.collectionToCellValue(Arrays.asList(1L, null, 2L)));
	}

	@Test
	void handlerUsesTheSupplierParsersAndReportsUnknownBooleansAsNull() {
		DefaultDelimitedListHandler defaults = new DefaultDelimitedListHandler(';');
		Assertions.assertEquals(Arrays.asList(true, null, false), defaults.cellValueToList(Boolean.class, "Y;maybe;F"));
		Assertions.assertEquals(EXPECTED_DATES, defaults.cellValueToList(Date.class, DATE_LIST));

		DefaultDelimitedListHandler custom = new DefaultDelimitedListHandler(',', CellParsers.defaults().withDateParser(COMPACT_DATE_PARSER).withBooleanParser(ONE_IS_TRUE));
		Assertions.assertEquals(List.of(Date.from(Instant.parse("2024-12-18T00:00:00Z"))), custom.cellValueToList(Date.class, "20241218"));
		Assertions.assertEquals(List.of(true, false), custom.cellValueToList(Boolean.class, "1,true"));
	}

	@Test
	void handlerNullCellAndUnsupportedClassBehaveAsBefore() {
		DefaultDelimitedListHandler handler = new DefaultDelimitedListHandler(';');
		Assertions.assertNull(handler.cellValueToList(Date.class, null));
		Assertions.assertNull(handler.cellValueToList(Boolean.class, null));
		Assertions.assertThrows(IllegalArgumentException.class, () -> handler.cellValueToList(Object.class, "x"));
		Assertions.assertThrows(DateTimeParseException.class, () -> handler.cellValueToList(Date.class, "not a date"));
	}

	@Test
	void sharedDefaultsMatchTheWholeCellRules() {
		CellParsers defaults = CellParsers.defaults();
		Assertions.assertEquals(Boolean.TRUE, defaults.booleanParser().apply("Yes"));
		Assertions.assertNull(defaults.booleanParser().apply("maybe"));
		Assertions.assertEquals(Date.from(Instant.parse("2024-12-18T08:00:00Z")), defaults.dateParser().apply("2024-12-18T08:00:00Z[Etc/UTC]"));
		// a zone-less value is read in the zone the parser was built for
		Assertions.assertEquals(Date.from(Instant.parse("2024-12-18T08:00:00Z")), CellParsers.isoDateParser(ZoneOffset.UTC).apply("2024-12-18T08:00:00"));
	}

	@Test
	void cellParsersAreImmutableAndReplaceOneParserAtATime() {
		CellParsers defaults = CellParsers.defaults();
		CellParsers dates = defaults.withDateParser(COMPACT_DATE_PARSER);
		Assertions.assertSame(COMPACT_DATE_PARSER, dates.dateParser());
		Assertions.assertSame(defaults.booleanParser(), dates.booleanParser());
		Assertions.assertSame(defaults, CellParsers.defaults(), "defaults are shared, not rebuilt");
		Assertions.assertThrows(NullPointerException.class, () -> defaults.withBooleanParser(null));
		Assertions.assertThrows(NullPointerException.class, () -> new CellParsers(ONE_IS_TRUE, null));
	}

	// ---- helpers

	private static final class FixedListHandler implements DelimitedListHandler {
		@Override
		@SuppressWarnings("unchecked")
		public <T> List<T> cellValueToList(Class<T> clazz, String cellValue) {
			return (List<T>) List.of("fixed");
		}

		@Override
		public String collectionToCellValue(Collection<?> collection) {
			return "fixed";
		}
	}
}
