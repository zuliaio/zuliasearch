package io.zulia.data.source.spreadsheet.delimited;

import io.zulia.data.common.HeaderConfig;
import io.zulia.data.input.DataInputStream;
import io.zulia.data.source.spreadsheet.DefaultDelimitedListHandler;
import io.zulia.data.source.spreadsheet.DelimitedListHandler;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.function.Function;

public class DelimitedSourceConfig {
	
	private Function<String, Boolean> booleanParser;
	private Function<String, Date> dateParser;
	
	private final DataInputStream dataInputStream;
	
	private HeaderConfig headerConfig;
	
	private DelimitedListHandler delimitedListHandler = new DefaultDelimitedListHandler(';');
	
	public DelimitedSourceConfig(DataInputStream dataInputStream) {
		this.dataInputStream = dataInputStream;
		this.booleanParser = (s) -> {
			String lowerCase = s.toLowerCase();
			return switch (lowerCase) {
				case "true", "t", "yes", "y", "1" -> true;
				case "false", "f", "no", "n", "0" -> false;
				default -> null;
			};
		};

		DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.systemDefault());
		this.dateParser = (s) -> Date.from(Instant.from(formatter.parse(s)));
	}
	
	public DelimitedSourceConfig withListDelimiter(char listDelimiter) {
		this.delimitedListHandler = new DefaultDelimitedListHandler(listDelimiter);
		return this;
	}
	
	public DelimitedSourceConfig withDelimitedListHandler(DelimitedListHandler delimitedListHandler) {
		this.delimitedListHandler = delimitedListHandler;
		return this;
	}
	
	public DelimitedSourceConfig withHeaders() {
		return withHeaders(new HeaderConfig());
	}
	
	public DelimitedSourceConfig withHeaders(HeaderConfig headerConfig) {
		this.headerConfig = headerConfig;
		return this;
	}
	
	public DelimitedSourceConfig withoutHeaders() {
		this.headerConfig = null;
		return this;
	}
	
	public DataInputStream getDataInputStream() {
		return dataInputStream;
	}
	
	public boolean hasHeaders() {
		return headerConfig != null;
	}
	
	public DelimitedListHandler getDelimitedListHandler() {
		return delimitedListHandler;
	}
	
	public HeaderConfig getHeaderConfig() {
		return headerConfig;
	}
	
	public Function<String, Boolean> getBooleanParser() {
		return booleanParser;
	}
	
	public DelimitedSourceConfig withBooleanParser(Function<String, Boolean> booleanParser) {
		this.booleanParser = booleanParser;
		return this;
	}
	
	public Function<String, Date> getDateParser() {
		return dateParser;
	}
	
	public DelimitedSourceConfig withDateParser(Function<String, Date> dateParser) {
		this.dateParser = dateParser;
		return this;
	}
}
