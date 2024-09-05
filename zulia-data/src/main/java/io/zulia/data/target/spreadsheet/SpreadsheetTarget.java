package io.zulia.data.target.spreadsheet;

import io.zulia.data.target.spreadsheet.excel.cell.Link;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;

public abstract class SpreadsheetTarget<R, C extends SpreadsheetTargetConfig<?, ?>> implements AutoCloseable {
	
	private final SpreadsheetTargetConfig<R, C> dataConfig;
	
	public SpreadsheetTarget(SpreadsheetTargetConfig<R, C> dataConfig) {
		this.dataConfig = dataConfig;
	}
	
	public void appendValue(Collection<?> value) {
		dataConfig.getCollectionHandler().writeType(generateReference(), value);
	}
	
	public void appendValue(Boolean value) {
		dataConfig.getBooleanTypeHandler().writeType(generateReference(), value);
	}
	
	public void appendValue(Date value) {
		dataConfig.getDateTypeHandler().writeType(generateReference(), value);
	}
	
	public void appendValue(Number value) {
		dataConfig.getNumberTypeHandler().writeType(generateReference(), value);
	}
	
	public void appendValue(String value) {
		dataConfig.getStringTypeHandler().writeType(generateReference(), value);
	}
	
	public void appendValue(Link link) {
		dataConfig.getLinkTypeHandler().writeType(generateReference(), link);
	}
	
	public void appendGenericValue(Object o) {
		dataConfig.getDefaultTypeHandler().writeType(generateReference(), o);
	}
	
	protected void writeHeaders(Collection<String> headers) {
		SpreadsheetTypeHandler<R, String> headerCellHandler = dataConfig.getHeaderHandler();
		for (String header : headers) {
			headerCellHandler.writeType(generateReference(), header);
		}
		finishRow();
	}
	
	protected abstract R generateReference();
	
	public void appendLink(String label, String href) {
		appendValue(new Link(label, href));
	}
	
	public void appendValue(Object o) {
		switch (o) {
			case Collection<?> collection -> appendValue(collection);
			case Date date -> appendValue(date);
			case Number number -> appendValue(number);
			case Boolean bool -> appendValue(bool);
			case Link link -> appendValue(link);
			case String string -> appendValue(string);
			case null, default -> appendGenericValue(o);
		}
	}
	
	public void appendValues(String... values) {
		for (String value : values) {
			appendValue(value);
		}
	}
	
	public void writeRow(String... values) {
		for (String value : values) {
			appendValue(value);
		}
		finishRow();
	}
	
	public void writeRow(Object... values) {
		for (Object value : values) {
			appendValue(value);
		}
		finishRow();
	}
	
	public void writeRow(Collection<?> values) {
		for (Object value : values) {
			appendValue(value);
		}
		finishRow();
	}
	
	public abstract void finishRow();
	
	public abstract void close() throws IOException;
}
