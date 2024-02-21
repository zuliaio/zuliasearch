package io.zulia.data.source.json;

public class ThrowingJsonLineParseExceptionHandler implements JsonLineParseExceptionHandler {
	@Override
	public JsonDataSourceRecord handleException(Exception e) {
		throw new RuntimeException(e);
	}
}
