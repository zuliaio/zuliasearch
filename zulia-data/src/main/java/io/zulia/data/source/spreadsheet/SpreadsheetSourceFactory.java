package io.zulia.data.source.spreadsheet;

import io.zulia.data.common.SpreadsheetType;
import io.zulia.data.input.DataInputStream;
import io.zulia.data.input.FileDataInputStream;
import io.zulia.data.source.spreadsheet.csv.CSVSource;
import io.zulia.data.source.spreadsheet.csv.CSVSourceConfig;
import io.zulia.data.source.spreadsheet.excel.ExcelSource;
import io.zulia.data.source.spreadsheet.excel.ExcelSourceConfig;

import java.io.IOException;

public class SpreadsheetSourceFactory {
	
	public static SpreadsheetSource<?> fromFileWithoutHeaders(String filePath) throws IOException {
		return fromStream(FileDataInputStream.from(filePath), false);
	}
	
	public static SpreadsheetSource<?> fromFileWithHeaders(String filePath) throws IOException {
		return fromStream(FileDataInputStream.from(filePath), true);
	}
	
	public static SpreadsheetSource<?> fromFile(String filePath, boolean hasHeaders) throws IOException {
		return fromStream(FileDataInputStream.from(filePath), hasHeaders);
	}
	
	public static SpreadsheetSource<?> fromStreamWithoutHeaders(DataInputStream dataInputStream) throws IOException {
		return fromStream(dataInputStream, false);
	}
	
	public static SpreadsheetSource<?> fromStreamWithHeaders(DataInputStream dataInputStream) throws IOException {
		return fromStream(dataInputStream, true);
	}
	
	public static SpreadsheetSource<?> fromStream(DataInputStream dataInputStream, boolean hasHeaders) throws IOException {
		SpreadsheetType spreadsheetType = SpreadsheetType.getSpreadsheetType(dataInputStream.getMeta());
		
		if (SpreadsheetType.CSV.equals(spreadsheetType) || SpreadsheetType.TSV.equals(spreadsheetType)) {
			CSVSourceConfig csvSourceConfig = CSVSourceConfig.from(dataInputStream);
			if (hasHeaders) {
				csvSourceConfig.withHeaders();
			}
			if (SpreadsheetType.TSV.equals(spreadsheetType)) {
				csvSourceConfig.withDelimiter('\t');
			}
			return CSVSource.withConfig(csvSourceConfig);
		}
		else if (SpreadsheetType.XLSX.equals(spreadsheetType) || SpreadsheetType.XLS.equals(spreadsheetType)) {
			ExcelSourceConfig excelSourceConfig = ExcelSourceConfig.from(dataInputStream);
			if (hasHeaders) {
				excelSourceConfig.withHeaders();
			}
			return ExcelSource.withConfig(excelSourceConfig);
		}
		else {
			throw new IllegalArgumentException("Failed to determine file type from content type <" + dataInputStream.getMeta()
							.contentType() + "> with filename <" + dataInputStream.getMeta().fileName() + ">");
		}
	}
	
}
