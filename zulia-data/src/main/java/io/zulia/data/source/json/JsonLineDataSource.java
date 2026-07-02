package io.zulia.data.source.json;

import io.zulia.data.input.DataInputStream;
import io.zulia.data.source.DataSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

public class JsonLineDataSource implements DataSource<JsonSourceRecord>, AutoCloseable {
	private final JsonLineSourceConfig jsonLineSourceConfig;
	private BufferedReader reader;

	private String next;
	private boolean iterated;

	public static JsonLineDataSource withConfig(JsonLineSourceConfig jsonLineSourceConfig) throws IOException {
		return new JsonLineDataSource(jsonLineSourceConfig);
	}

	public static JsonLineDataSource withDefaults(DataInputStream dataInputStream) throws IOException {
		return withConfig(JsonLineSourceConfig.from(dataInputStream));
	}

	protected JsonLineDataSource(JsonLineSourceConfig jsonLineSourceConfig) throws IOException {
		this.jsonLineSourceConfig = jsonLineSourceConfig;
		open();
	}

	protected void open() throws IOException {
		InputStream inputStream = jsonLineSourceConfig.getDataInputStream().openInputStream();
		reader = new BufferedReader(new InputStreamReader(inputStream, jsonLineSourceConfig.getCharset().newDecoder()));
		next = reader.readLine();
	}

	public void reset() throws IOException {
		reader.close();
		open();
	}

	@Override
	public Iterator<JsonSourceRecord> iterator() {

		// open() already primed the first line in the constructor, so the first iterator() call must not
		// reopen (an empty source legitimately leaves next == null). Reset only to re-iterate, which only
		// succeeds for resettable inputs because single-use streams cannot be read twice.
		if (iterated) {
			try {
				reset();
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		iterated = true;

		return new Iterator<>() {

			@Override
			public boolean hasNext() {
				return (next != null);
			}

			@Override
			public JsonSourceRecord next() {
				// Advance the cursor before parsing so the current line is consumed exactly once.
				// Otherwise, a malformed line that a non-throwing handler skips would be re-read forever.
				String currentLine = next;
				try {
					next = reader.readLine();
				}
				catch (IOException e) {
					// The underlying stream failed. Terminate iteration (so a non-throwing handler does not
					// retry the same broken position forever), then route the error through the configured
					// handler: the default throwing handler surfaces it loudly, a logging handler skips quietly.
					next = null;
					return jsonLineSourceConfig.getExceptionHandler().handleException(e);
				}

				try {
					return new JsonSourceRecord(currentLine);
				}
				catch (Exception e) {
					return jsonLineSourceConfig.getExceptionHandler().handleException(e);
				}
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException("Iterator is read only");
			}
		};
	}

	@Override
	public void close() throws Exception {
		reader.close();
	}
}
