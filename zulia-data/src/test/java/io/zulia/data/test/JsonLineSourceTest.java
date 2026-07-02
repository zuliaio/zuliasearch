package io.zulia.data.test;

import io.zulia.data.input.SingleUseDataInputStream;
import io.zulia.data.source.json.JsonLineDataSource;
import io.zulia.data.source.json.JsonLineSourceConfig;
import io.zulia.data.source.json.JsonSourceRecord;
import io.zulia.data.source.json.LoggingJsonLineParseExceptionHandler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class JsonLineSourceTest {

	@Test
	public void parsesMultipleLines() throws Exception {
		String jsonl = """
				{"id": "a", "count": 1, "ratio": 1.5, "active": true}
				{"id": "b", "count": 2, "ratio": 2.5, "active": false}
				{"id": "c", "count": 3, "ratio": 3.5, "active": true}
				""";

		List<JsonSourceRecord> records = readAll(jsonl);

		Assertions.assertEquals(3, records.size());
		Assertions.assertEquals(List.of("a", "b", "c"), records.stream().map(r -> r.getString("id")).toList());
		Assertions.assertEquals(1, records.getFirst().getInt("count"));
		Assertions.assertEquals(Boolean.FALSE, records.get(1).getBoolean("active"));
	}

	@Test
	public void numericGettersAcceptAnyJsonNumberType() {
		// Document.parse maps whole numbers in int range to Integer, larger ones to Long, and
		// decimals to Double. The getters used to delegate to Document's casting getters, so
		// getLong("count") threw ClassCastException whenever the value happened to fit in an int.
		JsonSourceRecord record = new JsonSourceRecord("{\"count\": 42, \"big\": 5000000000, \"ratio\": 2.5}");

		Assertions.assertEquals(42L, record.getLong("count"));
		Assertions.assertEquals(42.0, record.getDouble("count"));
		Assertions.assertEquals(42.0f, record.getFloat("count"));
		Assertions.assertEquals(42, record.getInt("count"));

		Assertions.assertEquals(5_000_000_000L, record.getLong("big"));
		Assertions.assertEquals(2.5, record.getDouble("ratio"));
		Assertions.assertEquals(2.5f, record.getFloat("ratio"));

		// an out-of-int-range value surfaces clearly instead of silently wrapping
		Assertions.assertThrows(ArithmeticException.class, () -> record.getInt("big"));

		Assertions.assertNull(record.getLong("missing"));
		Assertions.assertNull(record.getInt("missing"));
	}

	@Test
	public void skipsMalformedLineWithoutLooping() {
		// A non-throwing handler (logs and returns null) is the documented way to skip bad lines.
		// Before the fix, the cursor was not advanced on a parse failure, so the bad line was
		// re-parsed forever. The preemptive timeout fails the test if that regression returns.
		String jsonl = """
				{"id": "a"}
				{not valid json
				{"id": "c"}
				""";

		List<JsonSourceRecord> records = Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
			var dataInputStream = SingleUseDataInputStream.from(new ByteArrayInputStream(jsonl.getBytes(StandardCharsets.UTF_8)), "test.jsonl");
			JsonLineSourceConfig config = JsonLineSourceConfig.from(dataInputStream);
			config.withExceptionHandler(new LoggingJsonLineParseExceptionHandler(LoggerFactory.getLogger(JsonLineSourceTest.class)));

			List<JsonSourceRecord> collected = new ArrayList<>();
			try (JsonLineDataSource source = JsonLineDataSource.withConfig(config)) {
				for (JsonSourceRecord record : source) {
					collected.add(record);
				}
			}
			return collected;
		});

		// The bad line yields a null record from the logging handler; the two good lines parse normally.
		List<JsonSourceRecord> valid = records.stream().filter(Objects::nonNull).toList();
		Assertions.assertEquals(List.of("a", "c"), valid.stream().map(r -> r.getString("id")).toList());
	}

	@Test
	public void surfacesStreamErrorLoudlyByDefault() {
		// The default handler (ThrowingJsonLineParseExceptionHandler) must surface a failure of the
		// underlying stream as an exception rather than ending iteration silently.
		Assertions.assertThrows(RuntimeException.class, () -> {
			var dataInputStream = SingleUseDataInputStream.from(new FailAfterLineInputStream("{\"id\": \"a\"}\n"), "broken.jsonl");
			try (JsonLineDataSource source = JsonLineDataSource.withDefaults(dataInputStream)) {
				for (JsonSourceRecord ignored : source) {
					// drain until the stream failure surfaces
				}
			}
		});
	}

	@Test
	public void streamErrorCanBeSkippedQuietly() {
		// With a logging (non-throwing) handler the same stream failure ends iteration cleanly and
		// without looping. The preemptive timeout guards against a regression to the infinite loop.
		List<JsonSourceRecord> records = Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
			var dataInputStream = SingleUseDataInputStream.from(new FailAfterLineInputStream("{\"id\": \"a\"}\n"), "broken.jsonl");
			JsonLineSourceConfig config = JsonLineSourceConfig.from(dataInputStream);
			config.withExceptionHandler(new LoggingJsonLineParseExceptionHandler(LoggerFactory.getLogger(JsonLineSourceTest.class)));

			List<JsonSourceRecord> collected = new ArrayList<>();
			try (JsonLineDataSource source = JsonLineDataSource.withConfig(config)) {
				for (JsonSourceRecord record : source) {
					collected.add(record);
				}
			}
			return collected;
		});

		// Read-ahead means the failed read aborts before the in-hand line is returned, so the handler
		// contributes a null and iteration terminates with no surviving record.
		Assertions.assertTrue(records.stream().allMatch(Objects::isNull));
	}

	@Test
	public void emptySingleUseSourceIteratesEmptyWithoutReopening() throws Exception {
		// An empty source over a single-use stream must iterate to an empty result once. Previously iterator()
		// saw next == null and called reset(), which reopens the stream and throws on a single-use input.
		Assertions.assertEquals(0, readAll("").size());
	}

	private static List<JsonSourceRecord> readAll(String jsonl) throws Exception {
		var dataInputStream = SingleUseDataInputStream.from(new ByteArrayInputStream(jsonl.getBytes(StandardCharsets.UTF_8)), "test.jsonl");
		List<JsonSourceRecord> records = new ArrayList<>();
		try (JsonLineDataSource source = JsonLineDataSource.withDefaults(dataInputStream)) {
			for (JsonSourceRecord record : source) {
				records.add(record);
			}
		}
		return records;
	}

	/**
	 * Serves the bytes of a single line and then throws on the next read, simulating an underlying
	 * stream that fails partway through iteration.
	 */
	private static final class FailAfterLineInputStream extends InputStream {
		private final byte[] data;
		private int pos = 0;

		private FailAfterLineInputStream(String line) {
			this.data = line.getBytes(StandardCharsets.UTF_8);
		}

		@Override
		public int read() throws IOException {
			if (pos >= data.length) {
				throw new IOException("simulated stream failure");
			}
			return data[pos++] & 0xFF;
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			if (pos >= data.length) {
				throw new IOException("simulated stream failure");
			}
			int n = Math.min(len, data.length - pos);
			System.arraycopy(data, pos, b, off, n);
			pos += n;
			return n;
		}
	}
}
