package io.zulia.data.target.spreadsheet.csv;

import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import io.zulia.data.output.DataOutputStream;
import io.zulia.data.output.FileDataOutputStream;
import io.zulia.data.target.spreadsheet.SpreadsheetTarget;

import java.io.IOException;
import java.util.Collection;

public class CSVTarget extends SpreadsheetTarget<CsvWriter, CSVTargetConfig> {
	
	private final CsvWriterSettings settings;
	private final CSVTargetConfig csvDataTargetConfig;
	
	private CsvWriter csvWriter;
	
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
		settings = new CsvWriterSettings();
		settings.setMaxColumns(2048);
		open();
		
	}
	
	protected void open() throws IOException {
		csvWriter = new CsvWriter(csvDataTargetConfig.getDataStream().openOutputStream(), settings);
		if (csvDataTargetConfig.getHeaders() != null) {
			csvWriter.writeRow(csvDataTargetConfig.getHeaders());
		}
	}
	
	@Override
	protected CsvWriter generateReference() {
		return csvWriter;
	}
	
	public void finishRow() {
		csvWriter.writeValuesToRow();
	}
	
	public void close() {
		csvWriter.close();
	}
	
}
