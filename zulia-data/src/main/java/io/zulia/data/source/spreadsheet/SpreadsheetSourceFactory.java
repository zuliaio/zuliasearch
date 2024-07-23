package io.zulia.data.source.spreadsheet;

import io.zulia.data.DataStreamMeta;
import io.zulia.data.input.DataInputStream;
import io.zulia.data.source.spreadsheet.csv.CSVDataSource;
import io.zulia.data.source.spreadsheet.csv.CSVDataSourceConfig;
import io.zulia.data.source.spreadsheet.excel.ExcelDataSource;
import io.zulia.data.source.spreadsheet.excel.ExcelDataSourceConfig;

import java.io.IOException;

import static io.zulia.data.DataStreamMeta.isGzipExtension;

public class SpreadsheetSourceFactory {

	public enum SpreadsheetType {
		CSV,
		TSV,
		Excel
	}

	public static SpreadsheetType getSpreadsheetType(DataStreamMeta dataStreamMeta) {

		String contentType = dataStreamMeta.contentType().toLowerCase();
		switch (contentType) {
			case "text/csv" -> {
				return SpreadsheetType.CSV;
			}
			case "text/tab-separated-values" -> {
				return SpreadsheetType.TSV;
			}
			case "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "application/vnd.ms-excel" -> {
				return SpreadsheetType.Excel;
			}
		}

		String fileName = dataStreamMeta.fileName().toLowerCase();
		if (isGzipExtension(fileName)) {
			fileName = fileName.substring(0, fileName.length() - 3);
		}
		String extension = fileName.substring(fileName.length() - 3);

		return switch (extension) {
			case "csv" -> SpreadsheetType.CSV;
			case "tsv" -> SpreadsheetType.TSV;
			case "excel" -> SpreadsheetType.Excel;
			default -> null;
		};

	}

	public static SpreadsheetDataSource<?> getSpreadsheetSource(DataInputStream dataInputStream, boolean hasHeaders) throws IOException {
		SpreadsheetType spreadsheetType = getSpreadsheetType(dataInputStream.getMeta());

		if (SpreadsheetType.CSV.equals(spreadsheetType) || SpreadsheetType.TSV.equals(spreadsheetType)) {
			CSVDataSourceConfig csvDataSourceConfig = CSVDataSourceConfig.from(dataInputStream);
			if (hasHeaders) {
				csvDataSourceConfig.withHeaders();
			}
			if (SpreadsheetType.TSV.equals(spreadsheetType)) {
				csvDataSourceConfig.withDelimiter(',');
			}
			return CSVDataSource.withConfig(csvDataSourceConfig);
		}
		else if (SpreadsheetType.Excel.equals(spreadsheetType)) {
			ExcelDataSourceConfig excelDataSourceConfig = ExcelDataSourceConfig.from(dataInputStream);
			if (hasHeaders) {
				excelDataSourceConfig.withHeaders();
			}
			return ExcelDataSource.withConfig(excelDataSourceConfig);
		}
		else {
			throw new IllegalArgumentException(
					"Failed to determine file type from content type <" + dataInputStream.getMeta().contentType() + "> with filename <"
							+ dataInputStream.getMeta().fileName() + ">");
		}
	}

}
