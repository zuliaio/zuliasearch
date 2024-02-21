package io.zulia.data.source.json;

import io.zulia.data.input.DataInputStream;
import io.zulia.data.source.DataSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

public class JsonLineDataSource implements DataSource<JsonDataSourceRecord>, AutoCloseable {
	private final JsonLineDataSourceConfig jsonLineDataSourceConfig;
	private BufferedReader reader;

	private String next;

	public static JsonLineDataSource withConfig(JsonLineDataSourceConfig jsonLineDataSourceConfig) throws IOException {
		return new JsonLineDataSource(jsonLineDataSourceConfig);
	}

	public static JsonLineDataSource withDefaults(DataInputStream dataInputStream) throws IOException {
		return withConfig(JsonLineDataSourceConfig.from(dataInputStream));
	}

	protected JsonLineDataSource(JsonLineDataSourceConfig jsonLineDataSourceConfig) throws IOException {
		this.jsonLineDataSourceConfig = jsonLineDataSourceConfig;
		open();
	}

	protected void open() throws IOException {
		InputStream inputStream = jsonLineDataSourceConfig.getDataInputStream().openInputStream();
		reader = new BufferedReader(new InputStreamReader(inputStream, jsonLineDataSourceConfig.getCharset().newDecoder()));
		next = reader.readLine();
	}

	public void reset() throws IOException {
		reader.close();
		open();
	}

	@Override
	public Iterator<JsonDataSourceRecord> iterator() {

		return new Iterator<>() {

			@Override
			public boolean hasNext() {
				return (next != null);
			}

			@Override
			public JsonDataSourceRecord next() {
				try {
					JsonDataSourceRecord jsonDataSourceRecord = new JsonDataSourceRecord(next);
					next = reader.readLine();
					return jsonDataSourceRecord;
				}
				catch (Exception e) {
					return jsonLineDataSourceConfig.getExceptionHandler().handleException(e);
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
