package io.zulia.data.source.spreadsheet.excel;

import io.zulia.data.common.HeaderConfig;
import io.zulia.data.input.DataInputStream;
import io.zulia.data.source.spreadsheet.DelimitedListHandler;
import io.zulia.data.source.spreadsheet.DelimitedListSettings;

import java.util.Date;
import java.util.Objects;
import java.util.function.Function;

public class ExcelSourceConfig {

	public static ExcelSourceConfig from(DataInputStream dataStream) {
		return new ExcelSourceConfig(dataStream);
	}

	// the parsers reach text cells through the default cell handler and list elements through the list handler
	private final DelimitedListSettings listSettings = new DelimitedListSettings();

	private final DataInputStream dataInputStream;
	private HeaderConfig headerConfig;

	private OpenHandling openHandling = OpenHandling.FIRST_SHEET;

	// the cell handler is either one set explicitly or the default handler built from the parsers
	private ExcelCellHandler explicitCellHandler;
	private DefaultExcelCellHandler builtCellHandler;

	public enum OpenHandling {
		ACTIVE_SHEET,
		FIRST_SHEET
	}

	private ExcelSourceConfig(DataInputStream dataInputStream) {
		this.dataInputStream = dataInputStream;
	}

	public ExcelSourceConfig withHeaders() {
		return withHeaders(new HeaderConfig());
	}

	public ExcelSourceConfig withStrictHeaders() {
		return withHeaders(new HeaderConfig().allowBlanks(false).allowDuplicates(false));
	}

	public ExcelSourceConfig withHeaders(HeaderConfig headerConfig) {
		this.headerConfig = headerConfig;
		return this;
	}

	public ExcelSourceConfig withoutHeaders() {
		this.headerConfig = null;
		return this;
	}

	public ExcelSourceConfig withListDelimiter(char listDelimiter) {
		listSettings.withListDelimiter(listDelimiter);
		return this;
	}

	public ExcelSourceConfig withDelimitedListHandler(DelimitedListHandler delimitedListHandler) {
		listSettings.withHandler(delimitedListHandler);
		return this;
	}

	/**
	 * Replaces the default cell handler. A handler set here reads cells its own way and is not affected by {@link #withBooleanParser}
	 * or {@link #withDateParser}, which still apply to delimited lists inside a cell.
	 */
	public ExcelSourceConfig withExcelCellHandler(ExcelCellHandler excelCellHandler) {
		this.explicitCellHandler = Objects.requireNonNull(excelCellHandler, "excelCellHandler");
		return this;
	}

	/**
	 * Parses boolean text, both a text cell read with getBoolean and each element of a delimited list read with getList.
	 * Typed boolean cells are read from the cell type and do not go through the parser.
	 */
	public ExcelSourceConfig withBooleanParser(Function<String, Boolean> booleanParser) {
		listSettings.withParsers(listSettings.getParsers().withBooleanParser(booleanParser));
		builtCellHandler = null;
		return this;
	}

	/**
	 * Parses date text, both a text cell read with getDate and each element of a delimited list read with getList.
	 * Date formatted numeric cells are read from the cell type and do not go through the parser.
	 */
	public ExcelSourceConfig withDateParser(Function<String, Date> dateParser) {
		listSettings.withParsers(listSettings.getParsers().withDateParser(dateParser));
		builtCellHandler = null;
		return this;
	}

	public final OpenHandling getOpenHandling() {
		return openHandling;
	}

	public final ExcelSourceConfig setOpenHandling(OpenHandling openHandling) {
		this.openHandling = openHandling;
		return this;
	}

	public final DataInputStream getDataInputStream() {
		return dataInputStream;
	}

	public final boolean hasHeaders() {
		return headerConfig != null;
	}

	public DelimitedListHandler getDelimitedListHandler() {
		return listSettings.getHandler();
	}

	public ExcelCellHandler getExcelCellHandler() {
		if (explicitCellHandler != null) {
			return explicitCellHandler;
		}
		if (builtCellHandler == null) {
			builtCellHandler = new DefaultExcelCellHandler(listSettings.getParsers());
		}
		return builtCellHandler;
	}

	public HeaderConfig getHeaderConfig() {
		return headerConfig;
	}

	public Function<String, Boolean> getBooleanParser() {
		return listSettings.getParsers().booleanParser();
	}

	public Function<String, Date> getDateParser() {
		return listSettings.getParsers().dateParser();
	}
}
