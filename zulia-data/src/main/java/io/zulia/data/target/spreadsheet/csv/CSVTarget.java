package io.zulia.data.target.spreadsheet.csv;

import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import io.zulia.data.output.DataOutputStream;
import io.zulia.data.output.FileDataOutputStream;
import io.zulia.data.target.spreadsheet.delimited.DelimitedTarget;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

public class CSVTarget extends DelimitedTarget<CsvWriter, CSVTargetConfig> {
	
	private final CSVTargetConfig csvDataTargetConfig;
	private CsvWriterSettings settings;
	
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
		this.csvDataTargetConfig = csvDataTargetConfig;
	}
	
	@Override
	protected void init(CSVTargetConfig delimitedTargetConfig) {
		settings = new CsvWriterSettings();
		settings.setMaxColumns(2048);
		settings.getFormat().setDelimiter(csvDataTargetConfig.getDelimiter());
	}
	
	@Override
	protected CsvWriter createWriter(OutputStream outputStream) throws IOException {
		return new CsvWriter(outputStream, settings);
	}
	
}
