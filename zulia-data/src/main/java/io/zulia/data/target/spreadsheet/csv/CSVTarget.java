package io.zulia.data.target.spreadsheet.csv;

import de.siegmar.fastcsv.writer.CsvWriter;
import io.zulia.data.output.DataOutputStream;
import io.zulia.data.output.FileDataOutputStream;
import io.zulia.data.target.spreadsheet.delimited.DelimitedTarget;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;

public class CSVTarget extends DelimitedTarget<CSVTargetConfig> {

	private CsvWriter.CsvWriterBuilder csvWriterBuilder;

	public static CSVTarget withConfig(CSVTargetConfig csvDataTargetConfig) throws IOException {
		return new CSVTarget(csvDataTargetConfig);
	}

	public static CSVTarget withDefaults(DataOutputStream dataOutputStream) throws IOException {
		return withConfig(CSVTargetConfig.from(dataOutputStream));
	}

	public static CSVTarget withDefaultsFromFile(String path, boolean overwrite) throws IOException {
		return withDefaults(FileDataOutputStream.from(path, overwrite));
	}

	public static CSVTarget withDefaultsFromFile(String path, boolean overwrite, Collection<String> headers) throws IOException {
		return withConfig(CSVTargetConfig.from(FileDataOutputStream.from(path, overwrite)).withHeaders(headers));
	}

	protected CSVTarget(CSVTargetConfig csvDataTargetConfig) throws IOException {
		super(csvDataTargetConfig);
	}

	@Override
	protected void init(CSVTargetConfig csvDataTargetConfig) {
		csvWriterBuilder = CsvWriter.builder().autoFlush(true).fieldSeparator(csvDataTargetConfig.getDelimiter());
	}

	@Override
	protected CsvWriter createWriter(OutputStream outputStream) throws IOException {
		return csvWriterBuilder.build(outputStream);
	}

}
