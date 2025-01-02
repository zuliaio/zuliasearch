package io.zulia.data.source.spreadsheet.delimited;

import com.univocity.parsers.common.AbstractParser;
import io.zulia.data.common.HeaderMapping;
import io.zulia.data.source.spreadsheet.SpreadsheetSource;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.SequencedSet;

public abstract class DelimitedSource<T extends DelimitedRecord, S extends DelimitedSourceConfig> implements SpreadsheetSource<T>, AutoCloseable {
	private final S delimitedSourceConfig;
	private final AbstractParser<?> abstractParser;
	private String[] nextRow;
	private HeaderMapping headerMapping;

	public DelimitedSource(S delimitedSourceConfig) throws IOException {
		this.delimitedSourceConfig = delimitedSourceConfig;
		this.abstractParser = createParser(delimitedSourceConfig);
		open();
	}

	protected abstract AbstractParser<?> createParser(S delimitedSourceConfig);
	
	public void reset() throws IOException {
		abstractParser.stopParsing();
		open();
	}
	
	protected void open() throws IOException {
		abstractParser.beginParsing(new BufferedInputStream(delimitedSourceConfig.getDataInputStream().openInputStream()));
		
		if (delimitedSourceConfig.hasHeaders()) {
			
			String[] headerRow = abstractParser.parseNext();
			headerMapping = new HeaderMapping(delimitedSourceConfig.getHeaderConfig(), Arrays.stream(headerRow).toList());
			
		}
		
		nextRow = abstractParser.parseNext();
	}
	
	public boolean hasHeader(String field) {
		if (headerMapping == null) {
			throw new IllegalStateException("Cannot get field by name when headers where not read");
		}
		return headerMapping.hasHeader(field);
	}
	
	public SequencedSet<String> getHeaders() {
		if (headerMapping == null) {
			throw new IllegalStateException("Cannot get headers when headers where not read");
		}
		return headerMapping.getHeaderKeys();
	}
	
	@Override
	public Iterator<T> iterator() {
		
		//handles multiple iterations with the same DataSources
		if (nextRow == null) {
			try {
				reset();
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		
		return new Iterator<>() {
			
			@Override
			public boolean hasNext() {
				return (nextRow != null);
			}
			
			@Override
			public T next() {
				T t = createRecord(delimitedSourceConfig, nextRow);
				nextRow = abstractParser.parseNext();
				return t;
			}
			
			@Override
			public void remove() {
				throw new UnsupportedOperationException("Iterator is read only");
			}
		};
	}
	
	protected HeaderMapping getHeaderMapping() {
		return headerMapping;
	}

	protected abstract T createRecord(S delimitedSourceConfig, String[] nextRow);
	
	@Override
	public void close() {
		abstractParser.stopParsing();
	}
}
