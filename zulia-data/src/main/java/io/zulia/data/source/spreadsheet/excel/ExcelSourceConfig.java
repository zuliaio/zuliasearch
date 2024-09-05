package io.zulia.data.source.spreadsheet.excel;

import io.zulia.data.common.HeaderConfig;
import io.zulia.data.input.DataInputStream;
import io.zulia.data.source.spreadsheet.DefaultDelimitedListHandler;
import io.zulia.data.source.spreadsheet.DelimitedListHandler;

public class ExcelSourceConfig {
	
	public static ExcelSourceConfig from(DataInputStream dataStream) {
		return new ExcelSourceConfig(dataStream);
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
	
	private ExcelSourceConfig(DataInputStream dataInputStream) {
		this.dataInputStream = dataInputStream;
	}
	
	public ExcelSourceConfig withHeaders() {
		return withHeaders(new HeaderConfig());
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
		this.delimitedListHandler = new DefaultDelimitedListHandler(listDelimiter);
		return this;
	}
	
	public ExcelSourceConfig withDelimitedListHandler(DelimitedListHandler delimitedListHandler) {
		this.delimitedListHandler = delimitedListHandler;
		return this;
	}
	
	public ExcelSourceConfig withExcelCellHandler(ExcelCellHandler excelCellHandler) {
		this.excelCellHandler = excelCellHandler;
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
		return delimitedListHandler;
	}

	public ExcelCellHandler getExcelCellHandler() {
		return excelCellHandler;
	}

	public HeaderConfig getHeaderConfig() {
		return headerConfig;
	}
}
