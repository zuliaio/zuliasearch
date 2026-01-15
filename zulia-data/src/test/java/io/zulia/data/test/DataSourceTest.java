package io.zulia.data.test;

import io.zulia.data.input.SingleUseDataInputStream;
import io.zulia.data.output.SingleUseDataOutputStream;
import io.zulia.data.source.spreadsheet.SpreadsheetRecord;
import io.zulia.data.source.spreadsheet.SpreadsheetSource;
import io.zulia.data.source.spreadsheet.SpreadsheetSourceFactory;
import io.zulia.data.target.spreadsheet.SpreadsheetTargetFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

public class DataSourceTest {

	@Test
	void testCSV() throws IOException {

		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		SingleUseDataOutputStream outputStream = SingleUseDataOutputStream.from(byteArrayOutputStream, "test.csv");

		try (var target = SpreadsheetTargetFactory.fromStreamWithHeaders(outputStream, List.of("header1", "header2"))) {
			target.appendValue("value1");
			target.appendValue(1);
			target.finishRow();
			target.writeRow("value3", 1);
		}

		SingleUseDataInputStream dataInputStream = SingleUseDataInputStream.from(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()), "test.csv");
		try (SpreadsheetSource<?> dataSource = SpreadsheetSourceFactory.fromStreamWithHeaders(dataInputStream)) {
			Assertions.assertEquals("header1", dataSource.getHeaders().getFirst());
			Assertions.assertEquals("header2", dataSource.getHeaders().getLast());
			Assertions.assertEquals(2, dataSource.getHeaders().size());

			int i = 0;
			for (SpreadsheetRecord row : dataSource) {
				if (i == 0) {
					Assertions.assertEquals("value1", row.getString(0));
					Assertions.assertEquals(Integer.valueOf(1), row.getInt(1));
				}
				else if (i == 1) {
					Assertions.assertEquals("value3", row.getString(0));
				}
				else {
					Assertions.fail("Too many rows returned");
				}
				i++;
			}
		}
	}

	@Test
	void testEmptyCSV() throws IOException {

		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		SingleUseDataOutputStream outputStream = SingleUseDataOutputStream.from(byteArrayOutputStream, "test.csv");

		try (var target = SpreadsheetTargetFactory.fromStreamWithHeaders(outputStream, List.of("header1", "header2"))) {
			// just want to create a file with headers and no data rows
		}

		SingleUseDataInputStream dataInputStream = SingleUseDataInputStream.from(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()), "test.csv");
		try (SpreadsheetSource<?> dataSource = SpreadsheetSourceFactory.fromStreamWithHeaders(dataInputStream)) {
			Assertions.assertEquals("header1", dataSource.getHeaders().getFirst());
		}

	}

}
