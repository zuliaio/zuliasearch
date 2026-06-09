package io.zulia.data.test;

import io.zulia.data.input.SingleUseDataInputStream;
import io.zulia.data.source.spreadsheet.excel.ExcelRecord;
import io.zulia.data.source.spreadsheet.excel.ExcelSource;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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

	private static byte[] sheetToWorkbookAsBytes(Consumer<Sheet> sheetConsumer) throws IOException {
		try (XSSFWorkbook wb = new XSSFWorkbook(); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			sheetConsumer.accept(wb.createSheet("Sheet1"));
			wb.write(out);
			return out.toByteArray();
		}
	}

}
