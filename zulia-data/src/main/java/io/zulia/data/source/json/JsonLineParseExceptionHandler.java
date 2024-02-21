package io.zulia.data.source.json;

public interface JsonLineParseExceptionHandler {

	JsonDataSourceRecord handleException(Exception e);
}
