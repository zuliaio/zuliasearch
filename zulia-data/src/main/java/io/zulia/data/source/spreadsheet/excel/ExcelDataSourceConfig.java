package io.zulia.data.source.spreadsheet.excel;

import io.zulia.data.common.HeaderConfig;
import io.zulia.data.input.DataInputStream;
import io.zulia.data.source.spreadsheet.DefaultDelimitedListHandler;
import io.zulia.data.source.spreadsheet.DelimitedListHandler;

public class ExcelDataSourceConfig {

	public static ExcelDataSourceConfig from(DataInputStream dataStream) {
		return new ExcelDataSourceConfig(dataStream);
	}

	private DelimitedListHandler delimitedListHandler = new DefaultDelimitedListHandler(';');

	private final DataInputStream dataInputStream;
	private HeaderConfig headerConfig;

	private OpenHandling openHandling = OpenHandling.FIRST_SHEET;

	private ExcelCellHandler excelCellHandler = new DefaultExcelCellHandler();

	public enum OpenHandling {
		ACTIVE_SHEET,
		FIRST_SHEET
	}

	private ExcelDataSourceConfig(DataInputStream dataInputStream) {
		this.dataInputStream = dataInputStream;
	}

	public ExcelDataSourceConfig withHeaders() {
		return withHeaders(new HeaderConfig());
	}

	public ExcelDataSourceConfig withHeaders(HeaderConfig headerConfig) {
		this.headerConfig = headerConfig;
		return this;
	}

	public ExcelDataSourceConfig withoutHeaders() {
		this.headerConfig = null;
		return this;
	}

	public ExcelDataSourceConfig withListDelimiter(char listDelimiter) {
		this.delimitedListHandler = new DefaultDelimitedListHandler(listDelimiter);
		return this;
	}

	public ExcelDataSourceConfig withDelimitedListHandler(DelimitedListHandler delimitedListHandler) {
		this.delimitedListHandler = delimitedListHandler;
		return this;
	}

	public ExcelDataSourceConfig withExcelCellHandler(ExcelCellHandler excelCellHandler) {
		this.excelCellHandler = excelCellHandler;
		return this;
	}

	public final OpenHandling getOpenHandling() {
		return openHandling;
	}

	public final ExcelDataSourceConfig setOpenHandling(OpenHandling openHandling) {
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
		return delimitedListHandler;
	}

	public ExcelCellHandler getExcelCellHandler() {
		return excelCellHandler;
	}

	public HeaderConfig getHeaderConfig() {
		return headerConfig;
	}
}
