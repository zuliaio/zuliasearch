package io.zulia.data.source.spreadsheet.csv;

import io.zulia.data.input.FileDataInputStream;

import java.io.IOException;

public class DataSourceConfigFactory {

	public static CSVDataSourceConfig fromCSVFile(String filePath) throws IOException {
		return CSVDataSourceConfig.from(FileDataInputStream.from(filePath));
	}
}
