package io.zulia.data.test;

import io.zulia.data.input.FileDataInputStream;
import io.zulia.data.source.spreadsheet.SpreadsheetSource;
import io.zulia.data.source.spreadsheet.SpreadsheetSourceFactory;

import java.io.IOException;

public class Test {
	public static void main(String[] args) throws IOException {
		String file = "/data/Kyere COVID-19 RFI upload.xlsx";
		FileDataInputStream dataInputStream = FileDataInputStream.from(file);
		try (SpreadsheetSource<?> dataSource = SpreadsheetSourceFactory.fromStreamWithHeaders(dataInputStream)) {
			for (String header : dataSource.getHeaders()) {
				System.out.println("---" + header + "----");
			}
		}
	}
}
