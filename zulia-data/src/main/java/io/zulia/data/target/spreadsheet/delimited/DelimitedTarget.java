package io.zulia.data.target.spreadsheet.delimited;

import com.univocity.parsers.common.AbstractWriter;
import io.zulia.data.target.spreadsheet.SpreadsheetTarget;
import io.zulia.data.target.spreadsheet.SpreadsheetTargetConfig;

import java.io.IOException;
import java.io.OutputStream;

public abstract class DelimitedTarget<T extends AbstractWriter<?>, S extends DelimitedTargetConfig<T, S>> extends SpreadsheetTarget<T, S> {
	
	private final SpreadsheetTargetConfig<T, S> delimitedTargetConfig;
	
	private T delimitedWriter;
	
	protected DelimitedTarget(S delimitedTargetConfig) throws IOException {
		super(delimitedTargetConfig);
		this.delimitedTargetConfig = delimitedTargetConfig;
		init(delimitedTargetConfig);
		open();
		
	}
	
	protected abstract void init(S delimitedTargetConfig);
	
	protected void open() throws IOException {
		delimitedWriter = createWriter(delimitedTargetConfig.getDataStream().openOutputStream());
		if (delimitedTargetConfig.getHeaders() != null) {
			delimitedWriter.writeRow(delimitedTargetConfig.getHeaders());
		}
	}
	
	protected abstract T createWriter(OutputStream outputStream) throws IOException;
	
	@Override
	protected T generateReference() {
		return delimitedWriter;
	}
	
	public void finishRow() {
		delimitedWriter.writeValuesToRow();
	}
	
	public void close() {
		delimitedWriter.close();
	}
	
}
