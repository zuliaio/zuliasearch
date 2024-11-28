package io.zulia.data.source.spreadsheet.csv;

import com.univocity.parsers.common.AbstractParser;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import io.zulia.data.input.DataInputStream;
import io.zulia.data.source.spreadsheet.delimited.DelimitedSource;

import java.io.IOException;

public class CSVSource extends DelimitedSource<CSVRecord> {
	
	private final CSVSourceConfig csvSourceConfig;
	
	public static CSVSource withConfig(CSVSourceConfig csvSourceConfig) throws IOException {
		return new CSVSource(csvSourceConfig);
		
	}
	
	public static CSVSource withDefaults(DataInputStream dataInputStream) throws IOException {
		return CSVSource.withConfig(CSVSourceConfig.from(dataInputStream));
	}
	
	protected CSVSource(CSVSourceConfig csvSourceConfig) throws IOException {
		super(csvSourceConfig);
		this.csvSourceConfig = csvSourceConfig;
	}
	
	@Override
	protected AbstractParser<?> createParser() {
		CsvParserSettings parserSettings = new CsvParserSettings();
		parserSettings.setLineSeparatorDetectionEnabled(true);
		CsvFormat csvFormat = new CsvFormat();
		csvFormat.setDelimiter(csvSourceConfig.getDelimiter());
		parserSettings.setFormat(csvFormat);
		parserSettings.setMaxCharsPerColumn(100_000_000);
		parserSettings.setMaxColumns(10_000);
		return new CsvParser(parserSettings);
	}
	
	@Override
	protected CSVRecord createRecord(String[] nextRow) {
		return new CSVRecord(nextRow, getHeaderMapping(), csvSourceConfig);
	}
	
}
