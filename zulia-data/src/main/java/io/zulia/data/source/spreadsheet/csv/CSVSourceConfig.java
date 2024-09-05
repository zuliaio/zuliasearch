package io.zulia.data.source.spreadsheet.csv;

import io.zulia.data.common.HeaderConfig;
import io.zulia.data.input.DataInputStream;
import io.zulia.data.source.spreadsheet.DefaultDelimitedListHandler;
import io.zulia.data.source.spreadsheet.DelimitedListHandler;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.function.Function;

public class CSVSourceConfig {
	
	private Function<String, Boolean> booleanParser;
	private Function<String, Date> dateParser;
	
	public static CSVSourceConfig from(DataInputStream dataStream) {
		return new CSVSourceConfig(dataStream);
	}

	private final DataInputStream dataInputStream;
	private char delimiter = ',';

	private HeaderConfig headerConfig;

	private DelimitedListHandler delimitedListHandler = new DefaultDelimitedListHandler(';');
	
	private CSVSourceConfig(DataInputStream dataInputStream) {
		this.dataInputStream = dataInputStream;
		this.booleanParser = (s) -> {
			String lowerCase = s.toLowerCase();
			return switch (lowerCase) {
				case "true", "t", "yes", "y", "1" -> true;
				case "false", "f", "no", "n", "0" -> false;
				default -> null;
			};
		};
		
		DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE;
		this.dateParser = (s) -> Date.from(LocalDate.from(formatter.parse(s)).atStartOfDay().atOffset(ZoneOffset.UTC).toInstant());
	}
	
	public CSVSourceConfig withDelimiter(char delimiter) {
		this.delimiter = delimiter;
		return this;
	}
	
	public CSVSourceConfig withListDelimiter(char listDelimiter) {
		this.delimitedListHandler = new DefaultDelimitedListHandler(listDelimiter);
		return this;
	}
	
	public CSVSourceConfig withDelimitedListHandler(DelimitedListHandler delimitedListHandler) {
		this.delimitedListHandler = delimitedListHandler;
		return this;
	}
	
	public CSVSourceConfig withHeaders() {
		return withHeaders(new HeaderConfig());
	}
	
	public CSVSourceConfig withHeaders(HeaderConfig headerConfig) {
		this.headerConfig = headerConfig;
		return this;
	}
	
	public CSVSourceConfig withoutHeaders() {
		this.headerConfig = null;
		return this;
	}
	
	public DataInputStream getDataInputStream() {
		return dataInputStream;
	}

	public char getDelimiter() {
		return delimiter;
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
	
	public CSVSourceConfig withBooleanParser(Function<String, Boolean> booleanParser) {
		this.booleanParser = booleanParser;
		return this;
	}
	
	public Function<String, Date> getDateParser() {
		return dateParser;
	}
	
	public CSVSourceConfig withDateParser(Function<String, Date> dateParser) {
		this.dateParser = dateParser;
		return this;
	}
}
