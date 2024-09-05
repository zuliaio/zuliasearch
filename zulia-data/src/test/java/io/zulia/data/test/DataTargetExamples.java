package io.zulia.data.test;

import io.zulia.data.target.spreadsheet.SpreadsheetTarget;
import io.zulia.data.target.spreadsheet.SpreadsheetTargetFactory;
import io.zulia.data.target.spreadsheet.csv.CSVTarget;
import io.zulia.data.target.spreadsheet.excel.ExcelTarget;
import io.zulia.data.target.spreadsheet.excel.cell.Link;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Set;

public class DataTargetExamples {
	
	public static void main(String[] args) throws IOException {
		
		List<String> headers = List.of("string column", "number", "decimal number", "boolean column", "list column", "link column", "some date");
		try (CSVTarget csvTarget = CSVTarget.withDefaultsFromFile("/data/test.csv", true, headers)) {
			writeSampleRecords(csvTarget);
		}
		try (ExcelTarget excelTarget = ExcelTarget.withDefaultsFromFile("/data/test.xlsx", true, headers)) {
			writeSampleRecords(excelTarget);
		}
		
		try (SpreadsheetTarget<?, ?> spreadsheetTarget = SpreadsheetTargetFactory.fromFile("/data/test2.xlsx", true)) {
			writeSampleRecords(spreadsheetTarget);
		}
		
		try (SpreadsheetTarget<?, ?> spreadsheetTarget = SpreadsheetTargetFactory.fromFileWithHeaders("/data/test3.xlsx", true,
						List.of("header1", "header2"))) {
			writeSampleRecords(spreadsheetTarget);
		}
		
	}
	
	private static void writeSampleRecords(SpreadsheetTarget<?, ?> spreadsheetTarget) {
		spreadsheetTarget.appendValue("some string");
		spreadsheetTarget.appendValue(132);
		spreadsheetTarget.appendValue(10.332443f);
		spreadsheetTarget.appendValue(true);
		spreadsheetTarget.appendValue(List.of("a", "b", "c"));
		spreadsheetTarget.appendValue(new Link("google", "https://www.google.com")); //label is ignored by CSV by default
		spreadsheetTarget.appendValue(new Date());
		spreadsheetTarget.finishRow();
		
		spreadsheetTarget.appendValue("another string");
		spreadsheetTarget.appendValue(4443232333L);
		spreadsheetTarget.appendValue(10.33343432);
		spreadsheetTarget.appendValue(false);
		spreadsheetTarget.appendValue(Set.of("x", "y", "z"));
		spreadsheetTarget.appendValue(new Link("yahoo", "https://www.yahoo.com")); //label is ignored by CSV by default
		spreadsheetTarget.appendValue(new Date());
		spreadsheetTarget.finishRow();
	}
}
