package io.zulia.data.test;

import io.zulia.data.output.SingleUseDataOutputStream;
import io.zulia.data.target.spreadsheet.excel.ExcelTarget;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.util.DefaultTempFileCreationStrategy;
import org.apache.poi.util.TempFile;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.stream.Stream;

public class ExcelTargetTest {

	/**
	 * A LocalDate must land in a real date cell, styled like a Date, not in a text cell holding its toString. Both the
	 * typed overload and the Object dispatch used by writeRow are covered.
	 */
	@Test
	void localDateIsWrittenAsTypedDateCell() throws IOException {
		LocalDate day = LocalDate.of(2024, 5, 1);
		Date sameDay = Date.from(day.atStartOfDay(ZoneOffset.UTC).toInstant());

		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		try (ExcelTarget target = ExcelTarget.withDefaults(SingleUseDataOutputStream.from(bytes, "test.xlsx"))) {
			target.appendValue(day);
			target.appendValue((Object) day);
			target.appendValue(sameDay);
			target.appendValue((LocalDate) null);
			target.finishRow();
		}

		try (XSSFWorkbook workbook = new XSSFWorkbook(new ByteArrayInputStream(bytes.toByteArray()))) {
			Row row = workbook.getSheetAt(0).getRow(0);
			Cell typed = row.getCell(0);
			Cell dispatched = row.getCell(1);
			Cell fromDate = row.getCell(2);

			for (Cell cell : List.of(typed, dispatched)) {
				Assertions.assertEquals(CellType.NUMERIC, cell.getCellType());
				Assertions.assertTrue(DateUtil.isCellDateFormatted(cell), "expected a date formatted cell");
				Assertions.assertEquals(day, cell.getLocalDateTimeCellValue().toLocalDate());
				Assertions.assertEquals(fromDate.getCellStyle().getDataFormat(), cell.getCellStyle().getDataFormat());
			}
			Assertions.assertEquals(fromDate.getNumericCellValue(), typed.getNumericCellValue(), "same serial as the equivalent Date");
			Assertions.assertEquals(CellType.BLANK, row.getCell(3).getCellType());
		}
	}

	/**
	 * A LocalDateTime lands in a date cell with a date and time format, through the typed overload and the Object dispatch.
	 */
	@Test
	void localDateTimeIsWrittenAsTypedDateTimeCell() throws IOException {
		LocalDateTime at = LocalDateTime.of(2024, 5, 1, 13, 45, 30);

		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		try (ExcelTarget target = ExcelTarget.withDefaults(SingleUseDataOutputStream.from(bytes, "test.xlsx"))) {
			target.appendValue(at);
			target.appendValue((Object) at);
			target.appendValue((LocalDateTime) null);
			target.finishRow();
		}

		try (XSSFWorkbook workbook = new XSSFWorkbook(new ByteArrayInputStream(bytes.toByteArray()))) {
			Row row = workbook.getSheetAt(0).getRow(0);
			for (Cell cell : List.of(row.getCell(0), row.getCell(1))) {
				Assertions.assertEquals(CellType.NUMERIC, cell.getCellType());
				Assertions.assertTrue(DateUtil.isCellDateFormatted(cell), "expected a date formatted cell");
				Assertions.assertEquals(at, cell.getLocalDateTimeCellValue());
				Assertions.assertEquals("m/d/yy h:mm", cell.getCellStyle().getDataFormatString());
			}
			Assertions.assertEquals(CellType.BLANK, row.getCell(2).getCellType());
		}
	}

	@Test
	void closeDisposesStreamingTempFiles(@TempDir Path tempDir) throws IOException {
		// route POI's streaming temp files into the test's temp dir (thread-local so other tests are unaffected)
		TempFile.setThreadLocalTempFileCreationStrategy(new DefaultTempFileCreationStrategy(tempDir.toFile()));
		try {
			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			SingleUseDataOutputStream outputStream = SingleUseDataOutputStream.from(byteArrayOutputStream, "test.xlsx");

			try (ExcelTarget target = ExcelTarget.withDefaults(outputStream)) {
				// write well past the default in-memory window so rows spill to temp files on disk
				for (int i = 0; i < 2500; i++) {
					target.writeRow("value" + i, i);
				}
			}

			Assertions.assertTrue(byteArrayOutputStream.size() > 0, "expected workbook bytes to be written");

			// POI writes its streaming temp files directly into this dedicated dir, so after close() disposes them
			// nothing should remain
			try (Stream<Path> remaining = Files.list(tempDir)) {
				List<Path> leaked = remaining.toList();
				Assertions.assertTrue(leaked.isEmpty(), "temp dir should be empty after close, but found: " + leaked);
			}
		}
		finally {
			TempFile.setThreadLocalTempFileCreationStrategy(null);
		}
	}

}
