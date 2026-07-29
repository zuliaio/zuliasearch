package io.zulia.data.test;

import io.zulia.data.input.SingleUseDataInputStream;
import io.zulia.data.source.json.JsonSourceRecord;
import io.zulia.data.source.spreadsheet.SpreadsheetRecord;
import io.zulia.data.source.spreadsheet.SpreadsheetSource;
import io.zulia.data.source.spreadsheet.SpreadsheetSourceFactory;
import io.zulia.data.source.spreadsheet.excel.ExcelRecord;
import io.zulia.data.source.spreadsheet.excel.ExcelSource;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * Every data source accepts the same boolean forms the Zulia server accepts: true, t, yes, y, 1 and
 * false, f, no, n, 0, case insensitive, plus the native boolean and 1/0 numeric types where the format has them.
 */
public class BooleanFormatTest {

	@Test
	void delimitedAcceptsAllTextualForms() throws IOException {
		String csv = """
				flag,filler
				yes,x
				NO,x
				Y,x
				n,x
				T,x
				f,x
				true,x
				False,x
				1,x
				0,x
				 true ,x
				maybe,x
				,x
				""";

		List<Boolean> flags = new ArrayList<>();
		var dataInputStream = SingleUseDataInputStream.from(csv.getBytes(StandardCharsets.UTF_8), "test.csv");
		try (SpreadsheetSource<?> source = SpreadsheetSourceFactory.fromStreamWithHeaders(dataInputStream)) {
			for (SpreadsheetRecord record : source) {
				flags.add(record.getBoolean("flag"));
			}
		}

		// an unrecognized value and an empty cell both read as null rather than silently becoming false
		Assertions.assertEquals(Arrays.asList(true, false, true, false, true, false, true, false, true, false, true, null, null), flags);
	}

	@Test
	void excelAcceptsStringNumericBooleanAndFormulaCells() throws IOException {
		// string cells other than "true" used to go through Boolean.parseBoolean, so "yes" and "1" read as false,
		// and numeric 1/0 cells read as null because cellToBoolean had no numeric branch
		byte[] xlsx = sheetToWorkbookAsBytes(sheet -> {
			Row row = sheet.createRow(0);
			row.createCell(0).setCellValue("yes");
			row.createCell(1).setCellValue("N");
			row.createCell(2).setCellValue("1");
			row.createCell(3).setCellValue("0");
			row.createCell(4).setCellValue(1);
			row.createCell(5).setCellValue(0);
			row.createCell(6).setCellValue(true);
			row.createCell(7).setCellFormula("\"no\"");
			row.createCell(8).setCellFormula("1*1");
			row.createCell(9).setCellValue("maybe");
			row.createCell(10).setCellValue(7);
			sheet.getWorkbook().getCreationHelper().createFormulaEvaluator().evaluateAll();
		});

		try (ExcelSource source = ExcelSource.withDefaults(SingleUseDataInputStream.from(xlsx, "test.xlsx"))) {
			ExcelRecord record = source.iterator().next();
			Assertions.assertEquals(Boolean.TRUE, record.getBoolean(0));
			Assertions.assertEquals(Boolean.FALSE, record.getBoolean(1));
			Assertions.assertEquals(Boolean.TRUE, record.getBoolean(2));
			Assertions.assertEquals(Boolean.FALSE, record.getBoolean(3));
			Assertions.assertEquals(Boolean.TRUE, record.getBoolean(4));
			Assertions.assertEquals(Boolean.FALSE, record.getBoolean(5));
			Assertions.assertEquals(Boolean.TRUE, record.getBoolean(6));
			Assertions.assertEquals(Boolean.FALSE, record.getBoolean(7));
			Assertions.assertEquals(Boolean.TRUE, record.getBoolean(8));
			Assertions.assertNull(record.getBoolean(9));
			Assertions.assertNull(record.getBoolean(10));
			Assertions.assertNull(record.getBoolean(11));
		}
	}

	@Test
	void jsonAcceptsBooleanStringAndNumberValues() {
		// getBoolean delegated to Document.getBoolean, which cast the value and threw ClassCastException
		// on the string and number forms that JSON exports commonly use
		JsonSourceRecord record = new JsonSourceRecord("""
				{"native": true, "yes": "Yes", "n": "n", "one": 1, "zero": 0, "unknown": "maybe", "two": 2}
				""");

		Assertions.assertEquals(Boolean.TRUE, record.getBoolean("native"));
		Assertions.assertEquals(Boolean.TRUE, record.getBoolean("yes"));
		Assertions.assertEquals(Boolean.FALSE, record.getBoolean("n"));
		Assertions.assertEquals(Boolean.TRUE, record.getBoolean("one"));
		Assertions.assertEquals(Boolean.FALSE, record.getBoolean("zero"));
		Assertions.assertNull(record.getBoolean("unknown"));
		Assertions.assertNull(record.getBoolean("two"));
		Assertions.assertNull(record.getBoolean("missing"));
		Assertions.assertTrue(record.getBoolean("missing", true));
	}

	private static byte[] sheetToWorkbookAsBytes(Consumer<Sheet> sheetConsumer) throws IOException {
		try (XSSFWorkbook wb = new XSSFWorkbook(); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			sheetConsumer.accept(wb.createSheet("Sheet1"));
			wb.write(out);
			return out.toByteArray();
		}
	}
}
