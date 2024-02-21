package io.zulia.data.source.json;

import org.slf4j.Logger;

public class LoggingJsonLineParseExceptionHandler implements JsonLineParseExceptionHandler {

	private final Logger log;

	public LoggingJsonLineParseExceptionHandler(Logger log) {
		this.log = log;
	}

	@Override
	public JsonDataSourceRecord handleException(Exception e) {
		log.error(e.getMessage(), e);
		return null;
	}
}
