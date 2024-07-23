package io.zulia.data.source.spreadsheet.csv;

import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import io.zulia.data.common.HeaderMapping;
import io.zulia.data.input.DataInputStream;
import io.zulia.data.source.DataSource;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

public class CSVDataSource implements DataSource<CSVDataSourceRecord>, AutoCloseable {

	private final CSVDataSourceConfig csvDataSourceConfig;
	private final CsvParser csvParser;

	private String[] nextRow;
	private HeaderMapping headerMapping;

	public static CSVDataSource withConfig(CSVDataSourceConfig csvDataSourceConfig) throws IOException {
		return new CSVDataSource(csvDataSourceConfig);
	}

	public static CSVDataSource withDefaults(DataInputStream dataInputStream) throws IOException {
		return withConfig(CSVDataSourceConfig.from(dataInputStream));
	}

	protected CSVDataSource(CSVDataSourceConfig csvDataSourceConfig) throws IOException {
		this.csvDataSourceConfig = csvDataSourceConfig;
		CsvParserSettings parserSettings = new CsvParserSettings();
		parserSettings.setLineSeparatorDetectionEnabled(true);
		CsvFormat csvFormat = new CsvFormat();
		csvFormat.setDelimiter(csvDataSourceConfig.getDelimiter());
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
		csvParser.beginParsing(new BufferedInputStream(csvDataSourceConfig.getDataInputStream().openInputStream()));

		if (csvDataSourceConfig.hasHeaders()) {

			String[] headerRow = csvParser.parseNext();
			headerMapping = new HeaderMapping(csvDataSourceConfig.getHeaderConfig(), Arrays.stream(headerRow).toList());

		}

		nextRow = csvParser.parseNext();
	}

	public boolean hasHeader(String field) {
		if (headerMapping == null) {
			throw new IllegalStateException("Cannot get field by name when headers where not read");
		}
		return headerMapping.hasHeader(field);
	}

	public Collection<String> getHeaders() {
		if (headerMapping == null) {
			throw new IllegalStateException("Cannot get headers when headers where not read");
		}
		return headerMapping.getHeaderKeys();
	}

	@Override
	public Iterator<CSVDataSourceRecord> iterator() {

		return new Iterator<>() {

			@Override
			public boolean hasNext() {
				return (nextRow != null);
			}

			@Override
			public CSVDataSourceRecord next() {
				CSVDataSourceRecord csvDataSourceRecord = new CSVDataSourceRecord(nextRow, headerMapping, csvDataSourceConfig);
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
