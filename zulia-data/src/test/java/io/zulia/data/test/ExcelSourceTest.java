package io.zulia.data.test;

import io.zulia.data.input.SingleUseDataInputStream;
import io.zulia.data.source.spreadsheet.excel.ExcelRecord;
import io.zulia.data.source.spreadsheet.excel.ExcelSource;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class ExcelSourceTest {

	@Test
	void wholeNumberAboveIntMaxKeepsFullValueAsString() throws IOException {
		// 3 billion exceeds Integer.MAX_VALUE so previously formatNumericCellAsString truncated it to "2147483647"
		byte[] xlsx = sheetToWorkbookAsBytes(sheet -> sheet.createRow(0).createCell(0).setCellValue(3_000_000_000d));

		try (ExcelSource source = ExcelSource.withDefaults(SingleUseDataInputStream.from(xlsx, "test.xlsx"))) {
			ExcelRecord record = source.iterator().next();
			Assertions.assertEquals("3000000000", record.getString(0));
			Assertions.assertEquals(3_000_000_000L, record.getLong(0));
			// an out-of-int-range cell now surfaces clearly instead of silently wrapping to 2147483647
			Assertions.assertThrows(ArithmeticException.class, () -> record.getInt(0));
		}
	}

	@Test
	void emptySheetDoesNotThrowAndYieldsNoRows() throws IOException {
		byte[] xlsx = sheetToWorkbookAsBytes(sheet -> {
			// leave the sheet empty: constructing the source used to NPE on sheet.getRow(0)
		});

		try (ExcelSource source = ExcelSource.withDefaults(SingleUseDataInputStream.from(xlsx, "test.xlsx"))) {
			int rows = 0;
			for (ExcelRecord ignored : source) {
				rows++;
			}
			Assertions.assertEquals(0, rows);
		}
	}

	@Test
	void switchToUnknownSheetThrowsIllegalArgument() throws IOException {
		byte[] xlsx = sheetToWorkbookAsBytes(sheet -> sheet.createRow(0).createCell(0).setCellValue("value"));

		try (ExcelSource source = ExcelSource.withDefaults(SingleUseDataInputStream.from(xlsx, "test.xlsx"))) {
			Assertions.assertThrows(IllegalArgumentException.class, () -> source.switchSheet("missing"));
		}
	}

	@Test
	void blankInteriorRowDoesNotThrow() throws IOException {
		// Row 1 is intentionally never created, so POI's sheet.getRow(1) returns null on a blank interior row.
		// Accessing such a row used to NullPointerException. It now reads correctly as an empty row.
		byte[] xlsx = sheetToWorkbookAsBytes(sheet -> {
			sheet.createRow(0).createCell(0).setCellValue("first");
			sheet.createRow(2).createCell(0).setCellValue("third");
		});

		try (ExcelSource source = ExcelSource.withDefaults(SingleUseDataInputStream.from(xlsx, "test.xlsx"))) {
			List<String> firstColumn = new ArrayList<>();
			for (ExcelRecord record : source) {
				firstColumn.add(record.getString(0));
				Assertions.assertNotNull(record.getRow(), "getRow() must not throw on a blank row");
			}
			Assertions.assertEquals(Arrays.asList("first", null, "third"), firstColumn);
		}
	}

	@Test
	void presentButEmptyFirstRowDoesNotThrow() throws IOException {
		// Row 0 exists but has no cells, so POI's Row.getLastCellNum() returns -1. Before clamping, that made
		// numberOfColumns = -1 and ExcelRecord.getRow() did new String[-1] -> NegativeArraySizeException.
		byte[] xlsx = sheetToWorkbookAsBytes(sheet -> {
			sheet.createRow(0);
			sheet.createRow(1).createCell(0).setCellValue("data");
		});

		// a present-but-empty first row really does report -1 columns.
		try (Workbook workbook = WorkbookFactory.create(new ByteArrayInputStream(xlsx))) {
			Row firstRow = workbook.getSheetAt(0).getRow(0);
			Assertions.assertNotNull(firstRow, "expected a present-but-empty first row, not an absent one");
			Assertions.assertEquals(-1, firstRow.getLastCellNum());
		}

		try (ExcelSource source = ExcelSource.withDefaults(SingleUseDataInputStream.from(xlsx, "test.xlsx"))) {
			for (ExcelRecord record : source) {
				Assertions.assertNotNull(record.getRow());
			}
		}
	}

	@Test
	void booleanCellsReadAsStringsAndBooleans() throws IOException {
		// cellToString had no BOOLEAN branch, so TRUE/FALSE cells (and formulas with a cached
		// boolean result) read as null strings, silently dropping the column in whole-row exports.
		byte[] xlsx = sheetToWorkbookAsBytes(sheet -> {
			Row row = sheet.createRow(0);
			row.createCell(0).setCellValue(true);
			row.createCell(1).setCellValue(false);
			row.createCell(2).setCellFormula("1=1");
			sheet.getWorkbook().getCreationHelper().createFormulaEvaluator().evaluateAll();
		});

		try (ExcelSource source = ExcelSource.withDefaults(SingleUseDataInputStream.from(xlsx, "test.xlsx"))) {
			ExcelRecord record = source.iterator().next();
			Assertions.assertEquals("true", record.getString(0));
			Assertions.assertEquals("false", record.getString(1));
			Assertions.assertEquals("true", record.getString(2));
			Assertions.assertEquals(Boolean.TRUE, record.getBoolean(0));
			Assertions.assertEquals(Boolean.TRUE, record.getBoolean(2));
			Assertions.assertArrayEquals(new String[] { "true", "false", "true" }, record.getRow());
		}
	}

	@Test
	void largeIntegralNumbersAreNotClampedToLongRange() throws IOException {
		// formatNumericCellAsString rendered any integral double via longValue(), so 1E20 saturated
		// to Long.MAX_VALUE ("9223372036854775807") and exported a completely unrelated number.
		byte[] xlsx = sheetToWorkbookAsBytes(sheet -> {
			Row row = sheet.createRow(0);
			row.createCell(0).setCellValue(1e20);
			row.createCell(1).setCellValue(-1e20);
			row.createCell(2).setCellValue(42);
		});

		try (ExcelSource source = ExcelSource.withDefaults(SingleUseDataInputStream.from(xlsx, "test.xlsx"))) {
			ExcelRecord record = source.iterator().next();
			Assertions.assertEquals("1.0E20", record.getString(0));
			Assertions.assertEquals("-1.0E20", record.getString(1));
			Assertions.assertEquals("42", record.getString(2));
		}
	}

	private static byte[] sheetToWorkbookAsBytes(Consumer<Sheet> sheetConsumer) throws IOException {
		try (XSSFWorkbook wb = new XSSFWorkbook(); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			sheetConsumer.accept(wb.createSheet("Sheet1"));
			wb.write(out);
			return out.toByteArray();
		}
	}

}
