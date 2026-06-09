package io.zulia.data.test;

import io.zulia.data.common.DataStreamMeta;
import io.zulia.data.input.DataInputStream;
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
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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

	@Test
	void trulyEmptyDelimitedSourceWithHeadersThrowsIOException() throws IOException {
		// a file with no rows at all (not even a header line) should surface a clear IOException rather than letting
		// FastCSV's NoSuchElementException escape when headers are required
		SingleUseDataInputStream dataInputStream = SingleUseDataInputStream.from(new ByteArrayInputStream(new byte[0]), "test.csv");
		Assertions.assertThrows(IOException.class, () -> SpreadsheetSourceFactory.fromStreamWithHeaders(dataInputStream));
	}

	@Test
	void reIterationDoesNotLeakStreams() throws IOException {
		byte[] csv = "header1,header2\nvalue1,1\nvalue3,2\n".getBytes(StandardCharsets.UTF_8);
		CountingDataInputStream dataInputStream = new CountingDataInputStream(csv, "test.csv");

		try (SpreadsheetSource<?> dataSource = SpreadsheetSourceFactory.fromStreamWithHeaders(dataInputStream)) {
			Assertions.assertEquals(2, countRows(dataSource));
			// fully consuming then iterating again triggers reset()/open(); the previous reader must be closed first
			Assertions.assertEquals(2, countRows(dataSource));
		}

		Assertions.assertTrue(dataInputStream.openCount() >= 2, "re-iteration should have reopened the source");
		Assertions.assertEquals(0, dataInputStream.unclosedCount(), "every opened underlying stream should be closed");
	}

	private static int countRows(SpreadsheetSource<?> dataSource) {
		int rows = 0;
		for (SpreadsheetRecord ignored : dataSource) {
			rows++;
		}
		return rows;
	}

	/**
	 * A DataInputStream that hands back a fresh, tracked stream over the same bytes on every openRawInputStream() call so a
	 * test can assert how many streams were opened and that all of them were closed.
	 */
	private static final class CountingDataInputStream implements DataInputStream {
		private final byte[] data;
		private final DataStreamMeta meta;
		private final List<CountingInputStream> opened = new ArrayList<>();

		private CountingDataInputStream(byte[] data, String fileName) {
			this.data = data;
			this.meta = DataStreamMeta.fromFileName(fileName);
		}

		@Override
		public InputStream openRawInputStream() {
			CountingInputStream stream = new CountingInputStream(new ByteArrayInputStream(data));
			opened.add(stream);
			return stream;
		}

		@Override
		public DataStreamMeta getMeta() {
			return meta;
		}

		private int openCount() {
			return opened.size();
		}

		private long unclosedCount() {
			return opened.stream().filter(stream -> !stream.closed).count();
		}
	}

	private static final class CountingInputStream extends FilterInputStream {
		private boolean closed;

		private CountingInputStream(InputStream in) {
			super(in);
		}

		@Override
		public void close() throws IOException {
			closed = true;
			super.close();
		}
	}

}
