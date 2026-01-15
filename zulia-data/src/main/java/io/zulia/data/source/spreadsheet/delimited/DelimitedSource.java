package io.zulia.data.source.spreadsheet.delimited;

import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRecord;
import io.zulia.data.common.HeaderMapping;
import io.zulia.data.source.spreadsheet.SpreadsheetSource;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.SequencedSet;

public abstract class DelimitedSource<T extends DelimitedRecord, S extends DelimitedSourceConfig> implements SpreadsheetSource<T>, AutoCloseable {
	private final S delimitedSourceConfig;
	private final CsvReader.CsvReaderBuilder csvReaderBuilder;
	private CsvReader<CsvRecord> csvRecords;
	private List<String> nextRow;
	private HeaderMapping headerMapping;

	public DelimitedSource(S delimitedSourceConfig) throws IOException {
		this.delimitedSourceConfig = delimitedSourceConfig;
		this.csvReaderBuilder = createParser(delimitedSourceConfig);
		open();
	}

	protected abstract CsvReader.CsvReaderBuilder createParser(S delimitedSourceConfig);

	public void reset() throws IOException {
		//csvReaderBuilder.stopParsing();
		open();
	}

	protected void open() throws IOException {
		csvRecords = csvReaderBuilder.ofCsvRecord(new BufferedInputStream(delimitedSourceConfig.getDataInputStream().openInputStream()));

		if (delimitedSourceConfig.hasHeaders()) {

			List<String> headerRow = csvRecords.iterator().next().getFields();
			headerMapping = new HeaderMapping(delimitedSourceConfig.getHeaderConfig(), headerRow);

		}

		if (csvRecords.iterator().hasNext()) {
			nextRow = csvRecords.iterator().next().getFields();
		}
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
				T t = createRecord(delimitedSourceConfig, nextRow.toArray(new String[0]));
				if (csvRecords.iterator().hasNext()) {
					nextRow = csvRecords.iterator().next().getFields();
				}
				else {
					nextRow = null;
				}
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
		try {
			csvRecords.close();
		}
		catch (IOException e) {
			throw new RuntimeException("Could not close the CSV Records", e);
		}
	}
}
