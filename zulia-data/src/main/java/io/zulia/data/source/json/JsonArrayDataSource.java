package io.zulia.data.source.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.zulia.data.input.DataInputStream;
import io.zulia.data.source.DataSource;

import java.io.IOException;
import java.util.Iterator;

public class JsonArrayDataSource implements DataSource<JsonDataSourceRecord>, AutoCloseable {
	private final JsonArrayDataSourceConfig jsonArrayDataSourceConfig;

	private String next;
	private JsonParser parser;

	private final ObjectMapper mapper = new ObjectMapper();

	public static JsonArrayDataSource withConfig(JsonArrayDataSourceConfig jsonArrayDataSourceConfig) throws IOException {
		return new JsonArrayDataSource(jsonArrayDataSourceConfig);
	}

	public static JsonArrayDataSource withDefaults(DataInputStream dataInputStream) throws IOException {
		return withConfig(JsonArrayDataSourceConfig.from(dataInputStream));
	}

	protected JsonArrayDataSource(JsonArrayDataSourceConfig jsonArrayDataSourceConfig) throws IOException {
		this.jsonArrayDataSourceConfig = jsonArrayDataSourceConfig;
		open();
	}

	protected void open() throws IOException {

		parser = mapper.getFactory().createParser(jsonArrayDataSourceConfig.getDataInputStream().openInputStream());
		if (parser.nextToken() != JsonToken.START_ARRAY) {
			throw new IllegalStateException("Expected an array");
		}
		if (parser.nextToken() == JsonToken.START_OBJECT) {
			next = mapper.readTree(parser).toString();
		}

	}

	public void reset() throws IOException {
		close();
		open();
	}

	@Override
	public Iterator<JsonDataSourceRecord> iterator() {
		
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
			public JsonDataSourceRecord next() {
				JsonDataSourceRecord jsonDataSourceRecord = new JsonDataSourceRecord(next);
				try {
					if (parser.nextToken() == JsonToken.START_OBJECT) {
						next = mapper.readTree(parser).toString();
					}
					else {
						next = null;
					}
				}
				catch (IOException e) {
					System.out.println(e.getMessage());
					//throw new RuntimeException(e);
				}
				return jsonDataSourceRecord;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException("Iterator is read only");
			}
		};
	}

	@Override
	public void close() throws IOException {
		parser.close();
	}
}
