package io.zulia.data.target.spreadsheet;

import io.zulia.data.output.DataOutputStream;
import io.zulia.data.source.spreadsheet.DefaultDelimitedListHandler;
import io.zulia.data.source.spreadsheet.DelimitedListHandler;
import io.zulia.data.target.spreadsheet.excel.cell.Link;

import java.util.Collection;
import java.util.Map;

public abstract class SpreadsheetTargetConfig<T, S extends SpreadsheetTargetConfig<?, ?>> {

	private final DataOutputStream dataStream;

	private Collection<String> headers;

	private DelimitedListHandler delimitedListHandler = new DefaultDelimitedListHandler(';');

	private SpreadsheetTypeHandler<T, String> stringTypeHandler;
	private DateTypeHandler<T> dateTypeHandler;
	private SpreadsheetTypeHandler<T, Number> numberTypeHandler;
	private SpreadsheetTypeHandler<T, Boolean> booleanTypeHandler;
	private SpreadsheetTypeHandler<T, Collection<?>> collectionHandler;
	private SpreadsheetTypeHandler<T, Map<?, ?>> mapHandler;
	private SpreadsheetTypeHandler<T, Link> linkTypeHandler;
	private SpreadsheetTypeHandler<T, Object> defaultTypeHandler;
	private SpreadsheetTypeHandler<T, String> headerHandler;
	private SpreadsheetTypeHandler<T, String> boldHandler;
	private SpreadsheetTypeHandler<T, String> redBoldHandler;

	public SpreadsheetTargetConfig(DataOutputStream dataStream) {
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

	public DateTypeHandler<T> getDateTypeHandler() {
		return dateTypeHandler;
	}

	/**
	 * Handles Date, LocalDate and LocalDateTime. A lambda receives the local values as that day or wall clock time in UTC.
	 */
	public S withDateTypeHandler(DateTypeHandler<T> dateTypeHandler) {
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

	public SpreadsheetTypeHandler<T, Map<?, ?>> getMapHandler() {
		return mapHandler;
	}

	public S withMapHandler(SpreadsheetTypeHandler<T, Map<?, ?>> mapHandler) {
		this.mapHandler = mapHandler;
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

	public SpreadsheetTypeHandler<T, String> getBoldHandler() {
		return boldHandler;
	}

	public S withBoldHandler(SpreadsheetTypeHandler<T, String> boldHandler) {
		this.boldHandler = boldHandler;
		return getSelf();
	}

	public SpreadsheetTypeHandler<T, String> getRedBoldHandler() {
		return redBoldHandler;
	}

	public S withRedBoldHandler(SpreadsheetTypeHandler<T, String> redBoldHandler) {
		this.redBoldHandler = redBoldHandler;
		return getSelf();
	}
}
