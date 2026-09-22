package io.zulia.data.source.spreadsheet;

import io.zulia.data.common.HeaderConfig;
import io.zulia.data.input.DataInputStream;

import java.util.Date;
import java.util.function.Function;

/**
 * The configuration every spreadsheet source shares: the input stream, the header handling, the delimited list settings and the
 * cell parsers. {@link SpreadsheetSourceFactory} hands a caller this view of whichever type-specific config the stream selects,
 * so a source can be configured without knowing whether it is delimited or Excel.
 */
public interface SpreadsheetSourceConfig {

	DataInputStream getDataInputStream();

	SpreadsheetSourceConfig withHeaders();

	SpreadsheetSourceConfig withStrictHeaders();

	SpreadsheetSourceConfig withHeaders(HeaderConfig headerConfig);

	SpreadsheetSourceConfig withoutHeaders();

	boolean hasHeaders();

	HeaderConfig getHeaderConfig();

	SpreadsheetSourceConfig withListDelimiter(char listDelimiter);

	SpreadsheetSourceConfig withDelimitedListHandler(DelimitedListHandler delimitedListHandler);

	DelimitedListHandler getDelimitedListHandler();

	SpreadsheetSourceConfig withParsers(CellParsers parsers);

	SpreadsheetSourceConfig withBooleanParser(Function<String, Boolean> booleanParser);

	SpreadsheetSourceConfig withDateParser(Function<String, Date> dateParser);

	Function<String, Boolean> getBooleanParser();

	Function<String, Date> getDateParser();
}
