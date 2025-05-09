package io.zulia.data.target.spreadsheet;

import io.zulia.data.common.SpreadsheetType;
import io.zulia.data.output.DataOutputStream;
import io.zulia.data.output.FileDataOutputStream;
import io.zulia.data.target.spreadsheet.csv.CSVTarget;
import io.zulia.data.target.spreadsheet.csv.CSVTargetConfig;
import io.zulia.data.target.spreadsheet.excel.ExcelTarget;
import io.zulia.data.target.spreadsheet.excel.ExcelTargetConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;

public class SpreadsheetTargetFactory {

	public static SpreadsheetTarget<?, ?> fromFile(String filePath, boolean overwrite) throws IOException {
		return fromStream(FileDataOutputStream.from(filePath, overwrite));
	}

	public static SpreadsheetTarget<?, ?> fromPath(Path path, boolean overwrite) throws IOException {
		return fromStream(FileDataOutputStream.from(path, overwrite));
	}

	public static SpreadsheetTarget<?, ?> fromStream(DataOutputStream dataOutputStream) throws IOException {
		return fromStreamWithHeaders(dataOutputStream, null);
	}

	public static SpreadsheetTarget<?, ?> fromFileWithHeaders(String filePath, boolean overwrite, Collection<String> headers) throws IOException {
		return fromStreamWithHeaders(FileDataOutputStream.from(filePath, overwrite), headers);
	}

	public static SpreadsheetTarget<?, ?> fromPathWithHeaders(Path path, boolean overwrite, Collection<String> headers) throws IOException {
		return fromStreamWithHeaders(FileDataOutputStream.from(path, overwrite), headers);
	}

	public static SpreadsheetTarget<?, ?> fromStreamWithHeaders(DataOutputStream dataOutputStream, Collection<String> headers) throws IOException {
		SpreadsheetType spreadsheetType = SpreadsheetType.getSpreadsheetType(dataOutputStream.getMeta());

		if (SpreadsheetType.CSV.equals(spreadsheetType) || SpreadsheetType.TSV.equals(spreadsheetType)) {
			CSVTargetConfig csvDataTargetConfig = CSVTargetConfig.from(dataOutputStream);
			if (headers != null) {
				csvDataTargetConfig.withHeaders(headers);
			}
			if (SpreadsheetType.TSV.equals(spreadsheetType)) {
				csvDataTargetConfig.withDelimiter('\t');
			}
			return CSVTarget.withConfig(csvDataTargetConfig);
		}
		else if (SpreadsheetType.XLSX.equals(spreadsheetType)) {
			ExcelTargetConfig excelDataSourceConfig = ExcelTargetConfig.from(dataOutputStream);
			if (headers != null) {
				excelDataSourceConfig.withHeaders(headers);
			}
			return ExcelTarget.withConfig(excelDataSourceConfig);
		}
		else {
			throw new IllegalArgumentException("Failed to determine file type from content type " + dataOutputStream.getMeta()
							.contentType() + " with filename " + dataOutputStream.getMeta().fileName());
		}
	}
}
