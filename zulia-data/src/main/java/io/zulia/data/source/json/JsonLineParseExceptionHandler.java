package io.zulia.data.source.json;

public interface JsonLineParseExceptionHandler {

	JsonSourceRecord handleException(Exception e);
}
