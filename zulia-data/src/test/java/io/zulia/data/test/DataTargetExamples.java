package io.zulia.data.test;

import io.zulia.data.target.spreadsheet.SpreadsheetDataTarget;
import io.zulia.data.target.spreadsheet.csv.CSVDataTarget;
import io.zulia.data.target.spreadsheet.excel.ExcelDataTarget;
import io.zulia.data.target.spreadsheet.excel.cell.Link;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Set;

public class DataTargetExamples {
	
	public static void main(String[] args) throws IOException {
		
		List<String> headers = List.of("string column", "number", "decimal number", "boolean column", "list column", "link column", "some date");
		try (CSVDataTarget csvDataTarget = CSVDataTarget.withDefaultsFromFile("/data/test.csv", true, headers)) {
			writeSampleRecords(csvDataTarget);
		}
		try (ExcelDataTarget excelDataTarget = ExcelDataTarget.withDefaultsFromFile("/data/test.xlsx", true, headers)) {
			writeSampleRecords(excelDataTarget);
		}
		
	}
	
	private static void writeSampleRecords(SpreadsheetDataTarget<?, ?> spreadsheetDataTarget) {
		spreadsheetDataTarget.appendValue("some string");
		spreadsheetDataTarget.appendValue(132);
		spreadsheetDataTarget.appendValue(10.332443f);
		spreadsheetDataTarget.appendValue(true);
		spreadsheetDataTarget.appendValue(List.of("a", "b", "c"));
		spreadsheetDataTarget.appendValue(new Link("google", "https://www.google.com")); //label is ignored by CSV by default
		spreadsheetDataTarget.appendValue(new Date());
		spreadsheetDataTarget.finishRow();
		
		spreadsheetDataTarget.appendValue("another string");
		spreadsheetDataTarget.appendValue(4443232333L);
		spreadsheetDataTarget.appendValue(10.33343432);
		spreadsheetDataTarget.appendValue(false);
		spreadsheetDataTarget.appendValue(Set.of("x", "y", "z"));
		spreadsheetDataTarget.appendValue(new Link("yahoo", "https://www.yahoo.com")); //label is ignored by CSV by default
		spreadsheetDataTarget.appendValue(new Date());
		spreadsheetDataTarget.finishRow();
	}
}
