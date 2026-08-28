package io.zulia.data.test;

import io.zulia.data.input.SingleUseDataInputStream;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.util.LocaleUtil;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.TimeZone;
import java.util.function.Consumer;

/**
 * Builds small spreadsheet files in memory for the source tests.
 */
final class SpreadsheetTestFiles {

	private SpreadsheetTestFiles() {
	}

	/**
	 * Writes a one sheet workbook. ExcelSource reads dates in UTC through POI's thread local time zone, so the workbook is written
	 * in UTC as well, otherwise a typed date cell round-trips shifted by the test machine's offset.
	 */
	static byte[] workbook(Consumer<Sheet> sheetConsumer) throws IOException {
		TimeZone previous = LocaleUtil.getUserTimeZone();
		LocaleUtil.setUserTimeZone(LocaleUtil.TIMEZONE_UTC);
		try (XSSFWorkbook wb = new XSSFWorkbook(); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			sheetConsumer.accept(wb.createSheet("Sheet1"));
			wb.write(out);
			return out.toByteArray();
		}
		finally {
			LocaleUtil.setUserTimeZone(previous);
		}
	}

	static void header(Sheet sheet, String... names) {
		Row row = sheet.createRow(0);
		for (int i = 0; i < names.length; i++) {
			row.createCell(i).setCellValue(names[i]);
		}
	}

	static SingleUseDataInputStream csvStream(String csv) throws IOException {
		return SingleUseDataInputStream.from(new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)), "test.csv");
	}
}
