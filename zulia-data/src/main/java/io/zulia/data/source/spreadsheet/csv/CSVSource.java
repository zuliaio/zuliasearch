package io.zulia.data.source.spreadsheet.csv;

import de.siegmar.fastcsv.reader.CommentStrategy;
import de.siegmar.fastcsv.reader.CsvReader;
import io.zulia.data.input.DataInputStream;
import io.zulia.data.source.spreadsheet.delimited.DelimitedSource;

import java.io.IOException;

public class CSVSource extends DelimitedSource<CSVRecord, CSVSourceConfig> {

	public static CSVSource withConfig(CSVSourceConfig csvSourceConfig) throws IOException {
		return new CSVSource(csvSourceConfig);

	}

	public static CSVSource withDefaults(DataInputStream dataInputStream) throws IOException {
		return CSVSource.withConfig(CSVSourceConfig.from(dataInputStream));
	}

	protected CSVSource(CSVSourceConfig csvSourceConfig) throws IOException {
		super(csvSourceConfig);

	}

	@Override
	protected CsvReader.CsvReaderBuilder createParser(CSVSourceConfig csvSourceConfig) {
		/*
		CsvParserSettings parserSettings = new CsvParserSettings();
		parserSettings.setLineSeparatorDetectionEnabled(true);
		CsvFormat csvFormat = new CsvFormat();
		csvFormat.setDelimiter(csvSourceConfig.getDelimiter());
		parserSettings.setFormat(csvFormat);
		parserSettings.setMaxCharsPerColumn(100_000_000);
		parserSettings.setMaxColumns(10_000);
		return new CsvParser(parserSettings);
		 */

		return CsvReader.builder().fieldSeparator(csvSourceConfig.getDelimiter()).quoteCharacter('"').commentStrategy(CommentStrategy.SKIP)
				.commentCharacter('#').skipEmptyLines(true).allowExtraFields(false).allowMissingFields(false).allowExtraCharsAfterClosingQuote(false)
				.detectBomHeader(false).maxBufferSize(16777216);

	}

	@Override
	protected CSVRecord createRecord(CSVSourceConfig csvSourceConfig, String[] nextRow) {
		return new CSVRecord(nextRow, getHeaderMapping(), csvSourceConfig);
	}

}
