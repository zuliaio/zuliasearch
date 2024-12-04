package io.zulia.data.source.spreadsheet.tsv;

import com.univocity.parsers.common.AbstractParser;
import com.univocity.parsers.tsv.TsvFormat;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;
import io.zulia.data.input.DataInputStream;
import io.zulia.data.source.spreadsheet.csv.CSVSource;
import io.zulia.data.source.spreadsheet.csv.CSVSourceConfig;
import io.zulia.data.source.spreadsheet.delimited.DelimitedSource;

import java.io.IOException;

public class TSVSource extends DelimitedSource<TSVRecord, TSVSourceConfig> {

	public static TSVSource withConfig(TSVSourceConfig tsvSourceConfig) throws IOException {
		return new TSVSource(tsvSourceConfig);
	}

	public static CSVSource withDefaults(DataInputStream dataInputStream) throws IOException {
		return CSVSource.withConfig(CSVSourceConfig.from(dataInputStream));
	}

	protected TSVSource(TSVSourceConfig tsvSourceConfig) throws IOException {
		super(tsvSourceConfig);
	}

	@Override
	protected AbstractParser<?> createParser(TSVSourceConfig tsvSourceConfig) {
		TsvParserSettings parserSettings = new TsvParserSettings();
		parserSettings.setLineSeparatorDetectionEnabled(true);
		TsvFormat tsvFormat = new TsvFormat();
		parserSettings.setFormat(tsvFormat);
		parserSettings.setMaxCharsPerColumn(100_000_000);
		parserSettings.setMaxColumns(10_000);
		return new TsvParser(parserSettings);
	}

	@Override
	protected TSVRecord createRecord(TSVSourceConfig tsvSourceConfig, String[] nextRow) {
		return new TSVRecord(nextRow, getHeaderMapping(), tsvSourceConfig);
	}

}
