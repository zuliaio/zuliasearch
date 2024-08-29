package io.zulia.data.target.spreadsheet.excel;

import io.zulia.data.output.DataOutputStream;
import io.zulia.data.source.spreadsheet.DefaultDelimitedListHandler;
import io.zulia.data.source.spreadsheet.DelimitedListHandler;
import io.zulia.data.target.spreadsheet.excel.cell.BooleanCellHandler;
import io.zulia.data.target.spreadsheet.excel.cell.CellHandler;
import io.zulia.data.target.spreadsheet.excel.cell.CollectionCellHandler;
import io.zulia.data.target.spreadsheet.excel.cell.DateCellHandler;
import io.zulia.data.target.spreadsheet.excel.cell.DefaultCellHandler;
import io.zulia.data.target.spreadsheet.excel.cell.HeaderCellHandler;
import io.zulia.data.target.spreadsheet.excel.cell.Link;
import io.zulia.data.target.spreadsheet.excel.cell.LinkCellHandler;
import io.zulia.data.target.spreadsheet.excel.cell.NumberCellHandler;
import io.zulia.data.target.spreadsheet.excel.cell.StringCellHandler;

import java.util.Collection;
import java.util.Date;

public class ExcelDataTargetConfig {

	public static ExcelDataTargetConfig from(DataOutputStream dataStream) {
		return new ExcelDataTargetConfig(dataStream);
	}

	private final DataOutputStream dataStream;

	private char delimiter = ',';

	private Collection<String> headers;

	private DelimitedListHandler delimitedListHandler = new DefaultDelimitedListHandler(';');

	private CellHandler<String> stringCellHandler;
	private CellHandler<Date> dateCellHandler;
	private CellHandler<Number> numberCellHandler;
	private CellHandler<Boolean> booleanCellHandler;
	private CellHandler<Collection<?>> collectionCellHandler;
	private CellHandler<Link> linkCellHandler;
	private CellHandler<Object> defaultCellHandler;
	private CellHandler<String> headerCellHandler;
	private String primarySheetName;

	private ExcelDataTargetConfig(DataOutputStream dataStream) {
		this.dataStream = dataStream;
		this.stringCellHandler = new StringCellHandler();
		this.dateCellHandler = new DateCellHandler();
		this.numberCellHandler = new NumberCellHandler();
		this.linkCellHandler = new LinkCellHandler();
		this.defaultCellHandler = new DefaultCellHandler();
		this.booleanCellHandler = new BooleanCellHandler();
		this.collectionCellHandler = new CollectionCellHandler();
		this.headerCellHandler = new HeaderCellHandler();
	}

	public DataOutputStream getDataStream() {
		return dataStream;
	}

	public ExcelDataTargetConfig withDelimiter(char delimiter) {
		this.delimiter = delimiter;
		return this;
	}

	public ExcelDataTargetConfig withListDelimiter(char listDelimiter) {
		this.delimitedListHandler = new DefaultDelimitedListHandler(listDelimiter);
		return this;
	}

	public ExcelDataTargetConfig withDelimitedListHandler(DelimitedListHandler delimitedListHandler) {
		this.delimitedListHandler = delimitedListHandler;
		return this;
	}

	public ExcelDataTargetConfig withHeader(Collection<String> headers) {
		this.headers = headers;
		return this;
	}

	public Collection<String> getHeaders() {
		return headers;
	}

	public char getDelimiter() {
		return delimiter;
	}

	public DelimitedListHandler getDelimitedListHandler() {
		return delimitedListHandler;
	}

	public CellHandler<String> getStringCellHandler() {
		return stringCellHandler;
	}

	public ExcelDataTargetConfig withStringCellHandler(CellHandler<String> stringCellHandler) {
		this.stringCellHandler = stringCellHandler;
		return this;
	}

	public CellHandler<Date> getDateCellHandler() {
		return dateCellHandler;
	}

	public ExcelDataTargetConfig withDateCellHandler(CellHandler<Date> dateCellHandler) {
		this.dateCellHandler = dateCellHandler;
		return this;
	}

	public CellHandler<Number> getNumberCellHandler() {
		return numberCellHandler;
	}

	public ExcelDataTargetConfig withNumberCellHandler(CellHandler<Number> numberCellHandler) {
		this.numberCellHandler = numberCellHandler;
		return this;
	}

	public CellHandler<Boolean> getBooleanCellHandler() {
		return booleanCellHandler;
	}

	public ExcelDataTargetConfig withBooleanCellHandler(CellHandler<Boolean> booleanCellHandler) {
		this.booleanCellHandler = booleanCellHandler;
		return this;
	}

	public CellHandler<Collection<?>> getCollectionCellHandler() {
		return collectionCellHandler;
	}

	public ExcelDataTargetConfig withCollectionCellHandler(CellHandler<Collection<?>> collectionCellHandler) {
		this.collectionCellHandler = collectionCellHandler;
		return this;
	}

	public CellHandler<Link> getLinkCellHandler() {
		return linkCellHandler;
	}

	public ExcelDataTargetConfig withLinkCellHandler(CellHandler<Link> linkCellHandler) {
		this.linkCellHandler = linkCellHandler;
		return this;
	}

	public CellHandler<Object> getDefaultCellHandler() {
		return defaultCellHandler;
	}

	public ExcelDataTargetConfig withDefaultCellHandler(CellHandler<Object> defaultCellHandler) {
		this.defaultCellHandler = defaultCellHandler;
		return this;
	}

	public CellHandler<String> getHeaderCellHandler() {
		return headerCellHandler;
	}

	public ExcelDataTargetConfig withHeaderCellHandler(CellHandler<String> headerCellHandler) {
		this.headerCellHandler = headerCellHandler;
		return this;
	}

	public String getPrimarySheetName() {
		return primarySheetName;
	}

	public ExcelDataTargetConfig withPrimarySheetName(String primarySheetName) {
		this.primarySheetName = primarySheetName;
		return this;
	}
}
