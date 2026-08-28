package io.zulia.data.test;

import io.zulia.data.input.SingleUseDataInputStream;
import io.zulia.data.source.spreadsheet.csv.CSVSource;
import io.zulia.data.source.spreadsheet.csv.CSVSourceConfig;
import io.zulia.data.source.spreadsheet.delimited.DelimitedRecord;
import io.zulia.data.source.spreadsheet.excel.ExcelRecord;
import io.zulia.data.source.spreadsheet.excel.ExcelSource;
import io.zulia.data.source.spreadsheet.excel.ExcelSourceConfig;
import org.apache.poi.ss.usermodel.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.zulia.data.test.SpreadsheetTestFiles.csvStream;
import static io.zulia.data.test.SpreadsheetTestFiles.header;
import static io.zulia.data.test.SpreadsheetTestFiles.workbook;

/**
 * A blank cell in a numeric column reads as null rather than throwing NumberFormatException.
 * Excel keeps the STRING cell type for a cleared cell in a text formatted column, so the empty text has to be treated the same as a missing cell.
 */
public class BlankNumericCellTest {

	@Test
	void excelBlankStringCellsReadAsNullNumbers() throws IOException {
		byte[] xlsx = workbook(sheet -> {
			header(sheet, "id", "count", "amount");
			Row blank = sheet.createRow(1);
			blank.createCell(0).setCellValue("a");
			blank.createCell(1).setCellValue("");
			blank.createCell(2).setCellValue("   ");
			Row filled = sheet.createRow(2);
			filled.createCell(0).setCellValue("b");
			filled.createCell(1).setCellValue(" 42 ");
			filled.createCell(2).setCellValue("1.5");
		});

		try (ExcelSource source = ExcelSource.withConfig(ExcelSourceConfig.from(SingleUseDataInputStream.from(xlsx, "test.xlsx")).withHeaders())) {
			var iterator = source.iterator();
			ExcelRecord blank = iterator.next();
			Assertions.assertNull(blank.getLong("count"));
			Assertions.assertNull(blank.getInt("count"));
			Assertions.assertNull(blank.getDouble("amount"));
			Assertions.assertNull(blank.getFloat("amount"));
			Assertions.assertNull(blank.getLong(1));
			Assertions.assertNull(blank.getDouble(2));

			ExcelRecord filled = iterator.next();
			Assertions.assertEquals(42L, filled.getLong("count"));
			Assertions.assertEquals(42, filled.getInt("count"));
			Assertions.assertEquals(1.5d, filled.getDouble("amount"));
			Assertions.assertEquals(1.5f, filled.getFloat("amount"));
		}
	}

	@Test
	void excelNonNumericTextStillThrows() throws IOException {
		byte[] xlsx = workbook(sheet -> {
			header(sheet, "count");
			sheet.createRow(1).createCell(0).setCellValue("N/A");
		});

		try (ExcelSource source = ExcelSource.withConfig(ExcelSourceConfig.from(SingleUseDataInputStream.from(xlsx, "test.xlsx")).withHeaders())) {
			ExcelRecord record = source.iterator().next();
			Assertions.assertThrows(NumberFormatException.class, () -> record.getLong("count"));
		}
	}

	@Test
	void csvWhitespaceOnlyCellsReadAsNull() throws IOException {
		String csv = "id,count,amount,flag\na,,   ,\nb, 7 ,2.5,true\n";
		CSVSourceConfig config = CSVSourceConfig.from(csvStream(csv));
		config.withHeaders();
		try (CSVSource source = CSVSource.withConfig(config)) {
			var iterator = source.iterator();
			DelimitedRecord blank = iterator.next();
			Assertions.assertNull(blank.getLong("count"));
			Assertions.assertNull(blank.getDouble("amount"));
			Assertions.assertNull(blank.getDouble(2));
			Assertions.assertNull(blank.getBoolean("flag"));

			DelimitedRecord filled = iterator.next();
			Assertions.assertEquals(7L, filled.getLong("count"));
			Assertions.assertEquals(2.5d, filled.getDouble("amount"));
			Assertions.assertEquals(true, filled.getBoolean("flag"));
		}
	}

}
