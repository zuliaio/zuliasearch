package io.zulia.data.target.spreadsheet;

import io.zulia.data.common.SpreadsheetType;
import io.zulia.data.output.DataOutputStream;
import io.zulia.data.output.FileDataOutputStream;
import io.zulia.data.target.spreadsheet.csv.CSVDataTarget;
import io.zulia.data.target.spreadsheet.csv.CSVDataTargetConfig;
import io.zulia.data.target.spreadsheet.excel.ExcelDataTarget;
import io.zulia.data.target.spreadsheet.excel.ExcelDataTargetConfig;

import java.io.IOException;
import java.util.Collection;

public class SpreadsheetTargetFactory {
	
	public static SpreadsheetDataTarget<?, ?> fromFile(String filePath, boolean overwrite) throws IOException {
		return fromStream(FileDataOutputStream.from(filePath, overwrite));
	}
	
	public static SpreadsheetDataTarget<?, ?> fromStream(DataOutputStream dataOutputStream) throws IOException {
		return fromStreamWithHeaders(dataOutputStream, null);
	}
	
	public static SpreadsheetDataTarget<?, ?> fromFileWithHeaders(String filePath, boolean overwrite, Collection<String> headers) throws IOException {
		return fromStreamWithHeaders(FileDataOutputStream.from(filePath, overwrite), headers);
	}
	
	public static SpreadsheetDataTarget<?, ?> fromStreamWithHeaders(DataOutputStream dataOutputStream, Collection<String> headers) throws IOException {
		SpreadsheetType spreadsheetType = SpreadsheetType.getSpreadsheetType(dataOutputStream.getMeta());
		
		if (SpreadsheetType.CSV.equals(spreadsheetType) || SpreadsheetType.TSV.equals(spreadsheetType)) {
			CSVDataTargetConfig csvDataTargetConfig = CSVDataTargetConfig.from(dataOutputStream);
			if (headers != null) {
				csvDataTargetConfig.withHeaders(headers);
			}
			if (SpreadsheetType.TSV.equals(spreadsheetType)) {
				csvDataTargetConfig.withDelimiter('\t');
			}
			return CSVDataTarget.withConfig(csvDataTargetConfig);
		}
		else if (SpreadsheetType.XLSX.equals(spreadsheetType)) {
			ExcelDataTargetConfig excelDataSourceConfig = ExcelDataTargetConfig.from(dataOutputStream);
			if (headers != null) {
				excelDataSourceConfig.withHeaders(headers);
			}
			return ExcelDataTarget.withConfig(excelDataSourceConfig);
		}
		else {
			throw new IllegalArgumentException("Failed to determine file type from content type <" + dataOutputStream.getMeta()
							.contentType() + "> with filename <" + dataOutputStream.getMeta().fileName() + ">");
		}
	}
}
