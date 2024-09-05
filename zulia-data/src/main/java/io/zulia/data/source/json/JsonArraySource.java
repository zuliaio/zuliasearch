package io.zulia.data.source.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.zulia.data.input.DataInputStream;
import io.zulia.data.source.DataSource;

import java.io.IOException;
import java.util.Iterator;

public class JsonArraySource implements DataSource<JsonSourceRecord>, AutoCloseable {
	private final JsonArraySourceConfig jsonArraySourceConfig;

	private String next;
	private JsonParser parser;

	private final ObjectMapper mapper = new ObjectMapper();
	
	public static JsonArraySource withConfig(JsonArraySourceConfig jsonArraySourceConfig) throws IOException {
		return new JsonArraySource(jsonArraySourceConfig);
	}
	
	public static JsonArraySource withDefaults(DataInputStream dataInputStream) throws IOException {
		return withConfig(JsonArraySourceConfig.from(dataInputStream));
	}
	
	protected JsonArraySource(JsonArraySourceConfig jsonArraySourceConfig) throws IOException {
		this.jsonArraySourceConfig = jsonArraySourceConfig;
		open();
	}

	protected void open() throws IOException {
		
		parser = mapper.getFactory().createParser(jsonArraySourceConfig.getDataInputStream().openInputStream());
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
				JsonSourceRecord jsonSourceRecord = new JsonSourceRecord(next);
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
				return jsonSourceRecord;
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
