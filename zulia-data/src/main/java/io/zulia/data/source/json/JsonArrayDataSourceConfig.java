package io.zulia.data.source.json;

import io.zulia.data.input.DataInputStream;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class JsonArrayDataSourceConfig {

	private final DataInputStream dataInputStream;

	private Charset charset = StandardCharsets.UTF_8;

	public static JsonArrayDataSourceConfig from(DataInputStream dataInputStream) {
		return new JsonArrayDataSourceConfig(dataInputStream);
	}

	private JsonArrayDataSourceConfig(DataInputStream dataInputStream) {
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
}
