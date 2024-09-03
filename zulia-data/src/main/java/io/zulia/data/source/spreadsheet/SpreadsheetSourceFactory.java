package io.zulia.data.source.spreadsheet;

import io.zulia.data.common.SpreadsheetType;
import io.zulia.data.input.DataInputStream;
import io.zulia.data.source.spreadsheet.csv.CSVDataSource;
import io.zulia.data.source.spreadsheet.csv.CSVDataSourceConfig;
import io.zulia.data.source.spreadsheet.excel.ExcelDataSource;
import io.zulia.data.source.spreadsheet.excel.ExcelDataSourceConfig;

import java.io.IOException;

public class SpreadsheetSourceFactory {
	
	public static SpreadsheetDataSource<?> getSpreadsheetSource(DataInputStream dataInputStream, boolean hasHeaders) throws IOException {
		SpreadsheetType spreadsheetType = SpreadsheetType.getSpreadsheetType(dataInputStream.getMeta());
		
		if (SpreadsheetType.CSV.equals(spreadsheetType) || SpreadsheetType.TSV.equals(spreadsheetType)) {
			CSVDataSourceConfig csvDataSourceConfig = CSVDataSourceConfig.from(dataInputStream);
			if (hasHeaders) {
				csvDataSourceConfig.withHeaders();
			}
			if (SpreadsheetType.TSV.equals(spreadsheetType)) {
				csvDataSourceConfig.withDelimiter('\t');
			}
			return CSVDataSource.withConfig(csvDataSourceConfig);
		}
		else if (SpreadsheetType.XLSX.equals(spreadsheetType) || SpreadsheetType.XLS.equals(spreadsheetType)) {
			ExcelDataSourceConfig excelDataSourceConfig = ExcelDataSourceConfig.from(dataInputStream);
			if (hasHeaders) {
				excelDataSourceConfig.withHeaders();
			}
			return ExcelDataSource.withConfig(excelDataSourceConfig);
		}
		else {
			throw new IllegalArgumentException("Failed to determine file type from content type <" + dataInputStream.getMeta()
							.contentType() + "> with filename <" + dataInputStream.getMeta().fileName() + ">");
		}
	}
	
}
