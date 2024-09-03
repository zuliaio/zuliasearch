package io.zulia.data.common;

import static io.zulia.data.common.DataStreamMeta.isGzipExtension;

public enum SpreadsheetType {
	CSV,
	TSV,
	XLS,
	XLSX;
	
	public static SpreadsheetType getSpreadsheetType(DataStreamMeta dataStreamMeta) {
		
		String contentType = dataStreamMeta.contentType().toLowerCase();
		switch (contentType) {
			case "text/csv" -> {
				return SpreadsheetType.CSV;
			}
			case "text/tab-separated-values" -> {
				return SpreadsheetType.TSV;
			}
			case "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" -> {
				return SpreadsheetType.XLSX;
			}
			case "application/vnd.ms-excel" -> {
				return SpreadsheetType.XLS;
			}
		}
		
		String fileName = dataStreamMeta.fileName().toLowerCase();
		if (isGzipExtension(fileName)) {
			fileName = fileName.substring(0, fileName.length() - 3);
		}
		int lastIndexOfPeriod = fileName.lastIndexOf(".");
		String extension = lastIndexOfPeriod != -1 ? fileName.substring(lastIndexOfPeriod + 1) : "";
		
		return switch (extension) {
			case "csv" -> SpreadsheetType.CSV;
			case "tsv" -> SpreadsheetType.TSV;
			case "xls" -> SpreadsheetType.XLS;
			case "xlsx" -> SpreadsheetType.XLSX;
			default -> null;
		};
		
	}
}