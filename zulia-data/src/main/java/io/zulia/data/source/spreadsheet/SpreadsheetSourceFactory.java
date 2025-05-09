package io.zulia.data.source.spreadsheet;

import io.zulia.data.common.SpreadsheetType;
import io.zulia.data.input.DataInputStream;
import io.zulia.data.input.FileDataInputStream;
import io.zulia.data.source.spreadsheet.csv.CSVSource;
import io.zulia.data.source.spreadsheet.csv.CSVSourceConfig;
import io.zulia.data.source.spreadsheet.excel.ExcelSource;
import io.zulia.data.source.spreadsheet.excel.ExcelSourceConfig;
import io.zulia.data.source.spreadsheet.tsv.TSVSource;
import io.zulia.data.source.spreadsheet.tsv.TSVSourceConfig;

import java.io.IOException;

public class SpreadsheetSourceFactory {

	public enum HeaderOptions {
		STRICT,
		STANDARD,
		NONE
	}

	public static SpreadsheetSource<?> fromFileWithoutHeaders(String filePath) throws IOException {
		return fromStream(FileDataInputStream.from(filePath), HeaderOptions.NONE);
	}

	public static SpreadsheetSource<?> fromFileWithHeaders(String filePath) throws IOException {
		return fromStream(FileDataInputStream.from(filePath), HeaderOptions.STANDARD);
	}

	public static SpreadsheetSource<?> fromFileWithStrictHeaders(String filePath) throws IOException {
		return fromStream(FileDataInputStream.from(filePath), HeaderOptions.STRICT);
	}

	public static SpreadsheetSource<?> fromFile(String filePath, HeaderOptions headerOptions) throws IOException {
		return fromStream(FileDataInputStream.from(filePath), headerOptions);
	}

	public static SpreadsheetSource<?> fromStreamWithoutHeaders(DataInputStream dataInputStream) throws IOException {
		return fromStream(dataInputStream, HeaderOptions.NONE);
	}

	public static SpreadsheetSource<?> fromStreamWithHeaders(DataInputStream dataInputStream) throws IOException {
		return fromStream(dataInputStream, HeaderOptions.STANDARD);
	}

	public static SpreadsheetSource<?> fromStreamWithStrictHeaders(DataInputStream dataInputStream) throws IOException {
		return fromStream(dataInputStream, HeaderOptions.STRICT);
	}

	public static SpreadsheetSource<?> fromStream(DataInputStream dataInputStream, HeaderOptions headerOptions) throws IOException {
		SpreadsheetType spreadsheetType = SpreadsheetType.getSpreadsheetType(dataInputStream.getMeta());

		if (SpreadsheetType.CSV.equals(spreadsheetType)) {
			CSVSourceConfig csvSourceConfig = CSVSourceConfig.from(dataInputStream);
			if (HeaderOptions.STRICT.equals(headerOptions)) {
				csvSourceConfig.withStrictHeaders();
			}
			else if (HeaderOptions.STANDARD.equals(headerOptions)) {
				csvSourceConfig.withHeaders();
			}
			return CSVSource.withConfig(csvSourceConfig);
		}
		else if (SpreadsheetType.TSV.equals(spreadsheetType)) {
			TSVSourceConfig tsvSourceConfig = TSVSourceConfig.from(dataInputStream);
			if (HeaderOptions.STRICT.equals(headerOptions)) {
				tsvSourceConfig.withStrictHeaders();
			}
			else if (HeaderOptions.STANDARD.equals(headerOptions)) {
				tsvSourceConfig.withHeaders();
			}
			return TSVSource.withConfig(tsvSourceConfig);
		}
		else if (SpreadsheetType.XLSX.equals(spreadsheetType) || SpreadsheetType.XLS.equals(spreadsheetType)) {
			ExcelSourceConfig excelSourceConfig = ExcelSourceConfig.from(dataInputStream);
			if (HeaderOptions.STRICT.equals(headerOptions)) {
				excelSourceConfig.withStrictHeaders();
			}
			else if (HeaderOptions.STANDARD.equals(headerOptions)) {
				excelSourceConfig.withHeaders();
			}
			return ExcelSource.withConfig(excelSourceConfig);
		}
		else {
			throw new IllegalArgumentException(
					"Failed to determine file type from content type <" + dataInputStream.getMeta().contentType() + "> with filename <"
							+ dataInputStream.getMeta().fileName() + ">");
		}
	}

}
