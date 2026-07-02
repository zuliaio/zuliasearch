package io.zulia.data.source.json;

import io.zulia.data.input.DataInputStream;
import io.zulia.data.source.DataSource;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.util.Iterator;

public class JsonArraySource implements DataSource<JsonSourceRecord>, AutoCloseable {
	private final JsonArraySourceConfig jsonArraySourceConfig;

	private String next;
	private JsonParser parser;
	private boolean iterated;

	// Jackson 3 enables FAIL_ON_TRAILING_TOKENS by default; disable it so readTree can pull one object at a
	// time out of the array without treating the next object's START_OBJECT as a disallowed trailing token.
	private final JsonMapper mapper = JsonMapper.builder().disable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).build();

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

		parser = mapper.createParser(jsonArraySourceConfig.getDataInputStream().openInputStream());
		if (parser.nextToken() != JsonToken.START_ARRAY) {
			throw new IllegalStateException("Expected an array");
		}
		advanceToNextElement();
	}

	private void advanceToNextElement() {
		JsonToken token = parser.nextToken();
		if (token == JsonToken.START_OBJECT) {
			next = mapper.readTree(parser).toString();
		}
		else if (token == JsonToken.END_ARRAY || token == null) {
			// End of the array (or end of input): iteration is complete.
			next = null;
		}
		else {
			// A non-object element (scalar, null literal, or nested array). Fail loudly rather than silently
			// dropping this and every remaining element by mistaking it for the end of the array.
			throw new IllegalStateException("Expected an object in the JSON array but found " + token);
		}
	}

	public void reset() throws IOException {
		close();
		open();
	}

	@Override
	public Iterator<JsonSourceRecord> iterator() {

		// open() already primed the first record in the constructor, so the first iterator() call must not
		// reopen (an empty array legitimately leaves next == null). Re-iteration rewinds to the start, which
		// only succeeds for resettable inputs because single-use streams cannot be read twice.
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
				JsonSourceRecord jsonSourceRecord = new JsonSourceRecord(next);
				advanceToNextElement();
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
