package io.zulia.data.test;

import io.zulia.data.common.HeaderConfig;
import io.zulia.data.common.HeaderMapping;
import io.zulia.data.input.SingleUseDataInputStream;
import io.zulia.data.source.spreadsheet.SpreadsheetRecord;
import io.zulia.data.source.spreadsheet.SpreadsheetSource;
import io.zulia.data.source.spreadsheet.SpreadsheetSourceFactory;
import io.zulia.data.source.spreadsheet.SpreadsheetSourceFactory.HeaderOptions;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * {@link HeaderConfig#ignoreTrailingBlanks(boolean)} drops blank header cells after the last named header and nothing else,
 * and {@link HeaderMapping#getRawHeaders()} is read only for every source type.
 */
public class TrailingBlankHeaderTest {

	private static final HeaderConfig TRIM = new HeaderConfig().ignoreTrailingBlanks(true);
	private static final HeaderConfig STRICT_TRIM = new HeaderConfig().allowBlanks(false).allowDuplicates(false).ignoreTrailingBlanks(true);

	private static List<String> row(String... cells) {
		return new ArrayList<>(Arrays.asList(cells));
	}

	@Test
	void trailingBlanksAreKeptByDefault() {
		HeaderMapping mapping = new HeaderMapping(new HeaderConfig(), row("a", "b", "", null));
		Assertions.assertEquals(List.of("a", "b", "", "_2"), List.copyOf(mapping.getHeaderKeys()));
	}

	@Test
	void trailingBlanksAreDropped() {
		HeaderMapping mapping = new HeaderMapping(TRIM, row("a", "b", "", "  ", null));
		Assertions.assertEquals(List.of("a", "b"), List.copyOf(mapping.getHeaderKeys()));
		Assertions.assertEquals(List.of("a", "b"), mapping.getRawHeaders());
	}

	@Test
	void blankBetweenNamedHeadersIsKept() {
		HeaderMapping mapping = new HeaderMapping(TRIM, row("a", "", "c", ""));
		Assertions.assertEquals(List.of("a", "", "c"), List.copyOf(mapping.getHeaderKeys()));
		Assertions.assertEquals(2, mapping.getHeaderIndex("c"));
	}

	@Test
	void strictHeadersPassOnceTrailingBlanksAreDropped() {
		Assertions.assertEquals(List.of("a", "b"), List.copyOf(new HeaderMapping(STRICT_TRIM, row("a", "b", "", "")).getHeaderKeys()));
		Assertions.assertThrows(IllegalArgumentException.class, () -> new HeaderMapping(STRICT_TRIM, row("a", "", "c", "")));
	}

	@Test
	void rowOfOnlyBlanksIsStillRejected() {
		Assertions.assertThrows(IllegalArgumentException.class, () -> new HeaderMapping(TRIM, row("", null)));
	}

	@Test
	void rawHeadersAreReadOnly() {
		HeaderMapping mapping = new HeaderMapping(new HeaderConfig(), row("a", "b"));
		Assertions.assertThrows(UnsupportedOperationException.class, () -> mapping.getRawHeaders().set(0, "x"));
	}

	@Test
	void styledEmptyHeaderCellsInAWorkbook() throws IOException {
		byte[] workbook = SpreadsheetTestFiles.workbook(sheet -> {
			SpreadsheetTestFiles.header(sheet, "id", "name");
			// formatting applied two columns past the data leaves cells with a style and no value
			CellStyle bold = sheet.getWorkbook().createCellStyle();
			sheet.getRow(0).createCell(2).setCellStyle(bold);
			sheet.getRow(0).createCell(3).setCellStyle(bold);
			Row data = sheet.createRow(1);
			data.createCell(0).setCellValue("1");
			data.createCell(1).setCellValue("first");
		});

		try (SpreadsheetSource<?> source = SpreadsheetSourceFactory.fromStream(SingleUseDataInputStream.from(workbook, "test.xlsx"), HeaderOptions.STANDARD,
				null)) {
			Assertions.assertEquals(List.of("id", "name", "", "_2"), List.copyOf(source.getHeaders()));
		}
		try (SpreadsheetSource<?> source = SpreadsheetSourceFactory.fromStream(SingleUseDataInputStream.from(workbook, "test.xlsx"), HeaderOptions.STANDARD,
				config -> config.withHeaders(TRIM))) {
			Assertions.assertEquals(List.of("id", "name"), List.copyOf(source.getHeaders()));
			SpreadsheetRecord record = source.iterator().next();
			Assertions.assertEquals("first", record.getString("name"));
		}
	}

	@Test
	void trailingEmptyColumnsInADelimitedFile() throws IOException {
		try (SpreadsheetSource<?> source = SpreadsheetSourceFactory.fromStream(SpreadsheetTestFiles.csvStream("id,name,,\n1,first,,\n"),
				HeaderOptions.STANDARD, config -> config.withHeaders(TRIM))) {
			Assertions.assertEquals(List.of("id", "name"), List.copyOf(source.getHeaders()));
			Assertions.assertEquals("first", source.iterator().next().getString("name"));
		}
	}
}
