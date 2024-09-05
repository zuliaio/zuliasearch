package io.zulia.data.source.spreadsheet.csv;

import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import io.zulia.data.common.HeaderMapping;
import io.zulia.data.input.DataInputStream;
import io.zulia.data.source.spreadsheet.SpreadsheetSource;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.SequencedSet;

public class CSVSource implements SpreadsheetSource<CSVRecord>, AutoCloseable {
	
	private final CSVSourceConfig csvSourceConfig;
	private final CsvParser csvParser;

	private String[] nextRow;
	private HeaderMapping headerMapping;
	
	public static CSVSource withConfig(CSVSourceConfig csvSourceConfig) throws IOException {
		return new CSVSource(csvSourceConfig);
	}
	
	public static CSVSource withDefaults(DataInputStream dataInputStream) throws IOException {
		return withConfig(CSVSourceConfig.from(dataInputStream));
	}
	
	protected CSVSource(CSVSourceConfig csvSourceConfig) throws IOException {
		this.csvSourceConfig = csvSourceConfig;
		CsvParserSettings parserSettings = new CsvParserSettings();
		parserSettings.setLineSeparatorDetectionEnabled(true);
		CsvFormat csvFormat = new CsvFormat();
		csvFormat.setDelimiter(csvSourceConfig.getDelimiter());
		parserSettings.setFormat(csvFormat);
		parserSettings.setMaxCharsPerColumn(100_000_000);
		parserSettings.setMaxColumns(10_000);
		csvParser = new CsvParser(parserSettings);
		open();
	}

	public void reset() throws IOException {
		csvParser.stopParsing();
		open();
	}

	protected void open() throws IOException {
		csvParser.beginParsing(new BufferedInputStream(csvSourceConfig.getDataInputStream().openInputStream()));
		
		if (csvSourceConfig.hasHeaders()) {

			String[] headerRow = csvParser.parseNext();
			headerMapping = new HeaderMapping(csvSourceConfig.getHeaderConfig(), Arrays.stream(headerRow).toList());

		}

		nextRow = csvParser.parseNext();
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
	public Iterator<CSVRecord> iterator() {
		
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
			public CSVRecord next() {
				CSVRecord csvDataSourceRecord = new CSVRecord(nextRow, headerMapping, csvSourceConfig);
				nextRow = csvParser.parseNext();
				return csvDataSourceRecord;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException("Iterator is read only");
			}
		};
	}

	@Override
	public void close() {
		csvParser.stopParsing();
	}
}
