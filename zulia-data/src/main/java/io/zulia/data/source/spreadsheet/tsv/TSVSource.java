package io.zulia.data.source.spreadsheet.tsv;

import de.siegmar.fastcsv.reader.CommentStrategy;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.FieldMismatchStrategy;
import io.zulia.data.input.DataInputStream;
import io.zulia.data.source.spreadsheet.delimited.DelimitedSource;

import java.io.IOException;

public class TSVSource extends DelimitedSource<TSVRecord, TSVSourceConfig> {

	public static TSVSource withConfig(TSVSourceConfig tsvSourceConfig) throws IOException {
		return new TSVSource(tsvSourceConfig);
	}

	public static TSVSource withDefaults(DataInputStream dataInputStream) throws IOException {
		return TSVSource.withConfig(TSVSourceConfig.from(dataInputStream));
	}

	protected TSVSource(TSVSourceConfig tsvSourceConfig) throws IOException {
		super(tsvSourceConfig);
	}

	@Override
	protected CsvReader.CsvReaderBuilder createParser(TSVSourceConfig tsvSourceConfig) {
		return CsvReader.builder().fieldSeparator("\t").quoteCharacter('"').commentStrategy(CommentStrategy.SKIP).commentCharacter('#').skipEmptyLines(true)
				.extraFieldStrategy(FieldMismatchStrategy.STRICT).missingFieldStrategy(FieldMismatchStrategy.STRICT).allowExtraCharsAfterClosingQuote(false)
				.detectBomHeader(true).maxBufferSize(16777216);
	}

	@Override
	protected TSVRecord createRecord(TSVSourceConfig tsvSourceConfig, String[] nextRow) {
		return new TSVRecord(nextRow, getHeaderMapping(), tsvSourceConfig);
	}

}
