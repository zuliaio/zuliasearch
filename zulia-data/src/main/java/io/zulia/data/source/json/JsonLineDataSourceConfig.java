package io.zulia.data.source.json;

import io.zulia.data.input.DataInputStream;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class JsonLineDataSourceConfig {

	private final DataInputStream dataInputStream;

	private Charset charset = StandardCharsets.UTF_8;

	private JsonLineParseExceptionHandler exceptionHandler = new ThrowingJsonLineParseExceptionHandler();

	public static JsonLineDataSourceConfig from(DataInputStream dataInputStream) {
		return new JsonLineDataSourceConfig(dataInputStream);
	}

	private JsonLineDataSourceConfig(DataInputStream dataInputStream) {
		this.dataInputStream = dataInputStream;
	}

	public DataInputStream getDataInputStream() {
		return dataInputStream;
	}

	public void withCharset(Charset charset) {
		this.charset = charset;
	}

	public Charset getCharset() {
		return charset;
	}

	public JsonLineParseExceptionHandler getExceptionHandler() {
		return exceptionHandler;
	}

	public void withExceptionHandler(JsonLineParseExceptionHandler exceptionHandler) {
		this.exceptionHandler = exceptionHandler;
	}
}
