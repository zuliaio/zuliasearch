package io.zulia.data.target.spreadsheet;

import io.zulia.data.output.DataOutputStream;
import io.zulia.data.source.spreadsheet.DefaultDelimitedListHandler;
import io.zulia.data.source.spreadsheet.DelimitedListHandler;
import io.zulia.data.target.spreadsheet.excel.cell.Link;

import java.util.Collection;
import java.util.Date;

public abstract class SpreadsheetDataTargetConfig<T, S extends SpreadsheetDataTargetConfig<?, ?>> {
	
	private final DataOutputStream dataStream;
	
	private Collection<String> headers;
	
	private DelimitedListHandler delimitedListHandler = new DefaultDelimitedListHandler(';');
	
	private SpreadsheetTypeHandler<T, String> stringTypeHandler;
	private SpreadsheetTypeHandler<T, Date> dateTypeHandler;
	private SpreadsheetTypeHandler<T, Number> numberTypeHandler;
	private SpreadsheetTypeHandler<T, Boolean> booleanTypeHandler;
	private SpreadsheetTypeHandler<T, Collection<?>> collectionHandler;
	private SpreadsheetTypeHandler<T, Link> linkTypeHandler;
	private SpreadsheetTypeHandler<T, Object> defaultTypeHandler;
	private SpreadsheetTypeHandler<T, String> headerHandler;
	
	public SpreadsheetDataTargetConfig(DataOutputStream dataStream) {
		this.dataStream = dataStream;
		withListDelimiter(';');
	}
	
	public DataOutputStream getDataStream() {
		return dataStream;
	}
	
	protected abstract S getSelf();
	
	public S withListDelimiter(char listDelimiter) {
		this.delimitedListHandler = new DefaultDelimitedListHandler(listDelimiter);
		return getSelf();
	}
	
	public S withDelimitedListHandler(DelimitedListHandler delimitedListHandler) {
		this.delimitedListHandler = delimitedListHandler;
		return getSelf();
	}
	
	public S withHeaders(Collection<String> headers) {
		this.headers = headers;
		return getSelf();
	}
	
	public Collection<String> getHeaders() {
		return headers;
	}
	
	public DelimitedListHandler getDelimitedListHandler() {
		return delimitedListHandler;
	}
	
	public SpreadsheetTypeHandler<T, String> getStringTypeHandler() {
		return stringTypeHandler;
	}
	
	public S withStringHandler(SpreadsheetTypeHandler<T, String> stringTypeHandler) {
		this.stringTypeHandler = stringTypeHandler;
		return getSelf();
	}
	
	public SpreadsheetTypeHandler<T, Date> getDateTypeHandler() {
		return dateTypeHandler;
	}
	
	public S withDateTypeHandler(SpreadsheetTypeHandler<T, Date> dateTypeHandler) {
		this.dateTypeHandler = dateTypeHandler;
		return getSelf();
	}
	
	public SpreadsheetTypeHandler<T, Number> getNumberTypeHandler() {
		return numberTypeHandler;
	}
	
	public S withNumberTypeHandler(SpreadsheetTypeHandler<T, Number> numberTypeHandler) {
		this.numberTypeHandler = numberTypeHandler;
		return getSelf();
	}
	
	public SpreadsheetTypeHandler<T, Boolean> getBooleanTypeHandler() {
		return booleanTypeHandler;
	}
	
	public S withBooleanTypeHandler(SpreadsheetTypeHandler<T, Boolean> booleanTypeHandler) {
		this.booleanTypeHandler = booleanTypeHandler;
		return getSelf();
	}
	
	public SpreadsheetTypeHandler<T, Collection<?>> getCollectionHandler() {
		return collectionHandler;
	}
	
	public S withCollectionHandler(SpreadsheetTypeHandler<T, Collection<?>> collectionHandler) {
		this.collectionHandler = collectionHandler;
		return getSelf();
	}
	
	public SpreadsheetTypeHandler<T, Link> getLinkTypeHandler() {
		return linkTypeHandler;
	}
	
	public S withLinkTypeHandler(SpreadsheetTypeHandler<T, Link> linkTypeHandler) {
		this.linkTypeHandler = linkTypeHandler;
		return getSelf();
	}
	
	public SpreadsheetTypeHandler<T, Object> getDefaultTypeHandler() {
		return defaultTypeHandler;
	}
	
	public S withDefaultTypeHandler(SpreadsheetTypeHandler<T, Object> defaultTypeHandler) {
		this.defaultTypeHandler = defaultTypeHandler;
		return getSelf();
	}
	
	public SpreadsheetTypeHandler<T, String> getHeaderHandler() {
		return headerHandler;
	}
	
	public S withHeaderHandler(SpreadsheetTypeHandler<T, String> headerHandler) {
		this.headerHandler = headerHandler;
		return getSelf();
	}
}
