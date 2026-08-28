package io.zulia.data.source.spreadsheet.delimited;

import io.zulia.data.common.HeaderConfig;
import io.zulia.data.input.DataInputStream;
import io.zulia.data.source.spreadsheet.DelimitedListHandler;
import io.zulia.data.source.spreadsheet.DelimitedListSettings;

import java.util.Date;
import java.util.function.Function;

public class DelimitedSourceConfig {

	private final DataInputStream dataInputStream;

	private HeaderConfig headerConfig;

	private final DelimitedListSettings listSettings = new DelimitedListSettings();

	public DelimitedSourceConfig(DataInputStream dataInputStream) {
		this.dataInputStream = dataInputStream;
	}

	public DelimitedSourceConfig withListDelimiter(char listDelimiter) {
		listSettings.withListDelimiter(listDelimiter);
		return this;
	}

	public DelimitedSourceConfig withDelimitedListHandler(DelimitedListHandler delimitedListHandler) {
		listSettings.withHandler(delimitedListHandler);
		return this;
	}

	public DelimitedSourceConfig withHeaders() {
		return withHeaders(new HeaderConfig());
	}

	public DelimitedSourceConfig withStrictHeaders() {
		return withHeaders(new HeaderConfig().allowBlanks(false).allowDuplicates(false));
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
		return listSettings.getHandler();
	}

	public HeaderConfig getHeaderConfig() {
		return headerConfig;
	}

	public Function<String, Boolean> getBooleanParser() {
		return listSettings.getParsers().booleanParser();
	}

	public DelimitedSourceConfig withBooleanParser(Function<String, Boolean> booleanParser) {
		listSettings.withParsers(listSettings.getParsers().withBooleanParser(booleanParser));
		return this;
	}

	public Function<String, Date> getDateParser() {
		return listSettings.getParsers().dateParser();
	}

	public DelimitedSourceConfig withDateParser(Function<String, Date> dateParser) {
		listSettings.withParsers(listSettings.getParsers().withDateParser(dateParser));
		return this;
	}
}
