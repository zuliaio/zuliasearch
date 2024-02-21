package io.zulia.data.target.spreadsheet.csv;

import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import io.zulia.data.output.DataOutputStream;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

public class CSVDataTarget implements AutoCloseable {

	private final CsvWriterSettings settings;
	private final CSVDataTargetConfig csvDataTargetConfig;

	private CsvWriter csvWriter;

	public static CSVDataTarget withConfig(CSVDataTargetConfig csvDataTargetConfig) throws IOException {
		return new CSVDataTarget(csvDataTargetConfig);
	}

	public static CSVDataTarget withDefaults(DataOutputStream dataOutputStream) throws IOException {
		return withConfig(CSVDataTargetConfig.from(dataOutputStream));
	}

	protected CSVDataTarget(CSVDataTargetConfig csvDataTargetConfig) throws IOException {
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

	public void appendValue(Object o) {

	}

	public void appendValue(String value) {
		csvWriter.addValue(value);
	}

	public void appendValues(String... values) {
		csvWriter.addValues((Object[]) values);
	}

	public void finishRow() {
		csvWriter.writeValuesToRow();
	}

	public void writeRow(String... values) {
		csvWriter.writeRow(values);
	}

	public void writeRow(Object... values) {
		csvWriter.writeRow(Arrays.stream(values).map(o -> {
			if (o instanceof Collection<?> c) {
				if (c.isEmpty()) {
					return null;
				}
				return csvDataTargetConfig.getDelimitedListHandler().collectionToCellValue(c);
			}
			return o.toString();
		}).collect(Collectors.toList()));
	}

	public void close() {
		csvWriter.close();
	}

}
