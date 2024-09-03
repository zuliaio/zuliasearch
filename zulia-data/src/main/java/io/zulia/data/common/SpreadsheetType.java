package io.zulia.data.common;

import static io.zulia.data.common.DataStreamMeta.isGzipExtension;

public enum SpreadsheetType {
	CSV,
	TSV,
	XLS,
	XLSX;

	public static final String CSV_TYPE = "text/csv";
	public static final String TSV_TYPE = "text/tab-separated-values";
	public static final String XLS_TYPE = "application/vnd.ms-excel";
	public static final String XLSX_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";

	public static SpreadsheetType getSpreadsheetType(DataStreamMeta dataStreamMeta) {

		String contentType = dataStreamMeta.contentType().toLowerCase();
		switch (contentType) {
			case CSV_TYPE -> {
				return SpreadsheetType.CSV;
			}
			case TSV_TYPE -> {
				return SpreadsheetType.TSV;
			}
			case XLSX_TYPE -> {
				return SpreadsheetType.XLSX;
			}
			case XLS_TYPE -> {
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