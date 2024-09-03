package io.zulia.data.target.spreadsheet.csv;

import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import io.zulia.data.output.DataOutputStream;
import io.zulia.data.output.FileDataOutputStream;
import io.zulia.data.target.spreadsheet.SpreadsheetDataTarget;

import java.io.IOException;
import java.util.Collection;

public class CSVDataTarget extends SpreadsheetDataTarget<CsvWriter, CSVDataTargetConfig> {
	
	private final CsvWriterSettings settings;
	private final CSVDataTargetConfig csvDataTargetConfig;
	
	private CsvWriter csvWriter;
	
	public static CSVDataTarget withConfig(CSVDataTargetConfig csvDataTargetConfig) throws IOException {
		return new CSVDataTarget(csvDataTargetConfig);
	}
	
	public static CSVDataTarget withDefaults(DataOutputStream dataOutputStream) throws IOException {
		return withConfig(CSVDataTargetConfig.from(dataOutputStream));
	}
	
	public static CSVDataTarget withDefaultsFromFile(String path, boolean overwrite) throws IOException {
		return withDefaults(FileDataOutputStream.from(path, overwrite));
	}
	
	public static CSVDataTarget withDefaultsFromFile(String path, boolean overwrite, Collection<String> headers) throws IOException {
		return withConfig(CSVDataTargetConfig.from(FileDataOutputStream.from(path, overwrite)).withHeaders(headers));
	}
	
	protected CSVDataTarget(CSVDataTargetConfig csvDataTargetConfig) throws IOException {
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
