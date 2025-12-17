package io.zulia.data.target.spreadsheet.delimited;

import de.siegmar.fastcsv.writer.CsvWriter;
import io.zulia.data.target.spreadsheet.SpreadsheetTarget;
import io.zulia.data.target.spreadsheet.SpreadsheetTargetConfig;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public abstract class DelimitedTarget<S extends DelimitedTargetConfig<List<String>, S>> extends SpreadsheetTarget<List<String>, S> {

	private final SpreadsheetTargetConfig<List<String>, S> delimitedTargetConfig;
	private final List<String> reference;

	private CsvWriter delimitedWriter;

	protected DelimitedTarget(S delimitedTargetConfig) throws IOException {
		super(delimitedTargetConfig);
		this.delimitedTargetConfig = delimitedTargetConfig;
		reference = new ArrayList<>();
		init(delimitedTargetConfig);
		open();
	}

	protected abstract void init(S delimitedTargetConfig);

	protected void open() throws IOException {
		delimitedWriter = createWriter(delimitedTargetConfig.getDataStream().openOutputStream());
		if (delimitedTargetConfig.getHeaders() != null) {
			delimitedWriter.writeRecord(delimitedTargetConfig.getHeaders());
		}
	}

	protected abstract CsvWriter createWriter(OutputStream outputStream) throws IOException;

	@Override
	protected List<String> generateReference() {
		return reference;
	}

	public void finishRow() {
		delimitedWriter.writeRecord(reference);
		reference.clear();
	}

	public void close() {
		try {
			delimitedWriter.close();
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
