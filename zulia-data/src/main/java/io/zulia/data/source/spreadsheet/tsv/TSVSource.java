package io.zulia.data.source.spreadsheet.tsv;

import de.siegmar.fastcsv.reader.CommentStrategy;
import de.siegmar.fastcsv.reader.CsvReader;
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
	protected CsvReader.CsvReaderBuilder createParser(TSVSourceConfig tsvSourceConfig) {
		return CsvReader.builder().fieldSeparator("\t").quoteCharacter('"').commentStrategy(CommentStrategy.SKIP).commentCharacter('#').skipEmptyLines(true)
				.allowExtraFields(false).allowMissingFields(false).allowExtraCharsAfterClosingQuote(false).detectBomHeader(false).maxBufferSize(16777216);
		/*
		TsvParserSettings parserSettings = new TsvParserSettings();
		parserSettings.setLineSeparatorDetectionEnabled(true);
		TsvFormat tsvFormat = new TsvFormat();
		parserSettings.setFormat(tsvFormat);
		parserSettings.setMaxCharsPerColumn(100_000_000);
		parserSettings.setMaxColumns(10_000);
		return new TsvParser(parserSettings);
		 */
	}

	@Override
	protected TSVRecord createRecord(TSVSourceConfig tsvSourceConfig, String[] nextRow) {
		return new TSVRecord(nextRow, getHeaderMapping(), tsvSourceConfig);
	}

}
