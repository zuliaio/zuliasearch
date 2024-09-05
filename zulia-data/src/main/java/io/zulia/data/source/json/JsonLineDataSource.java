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
		
		if (next == null) {
			try {
				reset();
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		
		return new Iterator<>() {

			@Override
			public boolean hasNext() {
				return (next != null);
			}

			@Override
			public JsonSourceRecord next() {
				try {
					JsonSourceRecord jsonSourceRecord = new JsonSourceRecord(next);
					next = reader.readLine();
					return jsonSourceRecord;
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
