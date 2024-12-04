package io.zulia.data.test;

import io.zulia.data.output.FileDataOutputStream;
import io.zulia.data.target.spreadsheet.SpreadsheetTarget;
import io.zulia.data.target.spreadsheet.SpreadsheetTargetFactory;
import io.zulia.data.target.spreadsheet.csv.CSVTarget;
import io.zulia.data.target.spreadsheet.excel.ExcelTarget;
import io.zulia.data.target.spreadsheet.excel.ExcelTargetConfig;
import io.zulia.data.target.spreadsheet.excel.cell.Link;
import io.zulia.data.target.spreadsheet.tsv.TSVTarget;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Set;

public class DataTargetExamples {

	public static void csvTarget() throws IOException {
		List<String> headers = List.of("string column", "number", "decimal number", "boolean column", "list column", "link column", "some date");
		try (CSVTarget csvTarget = CSVTarget.withDefaultsFromFile("/data/test.csv", true, headers)) {
			writeSampleRecords(csvTarget);
		}
	}

	public static void tsvTarget() throws IOException {
		List<String> headers = List.of("string column", "number", "decimal number", "boolean column", "list column", "link column", "some date");
		try (TSVTarget tsvTarget = TSVTarget.withDefaultsFromFile("/data/test.tsv", true, headers)) {
			writeSampleRecords(tsvTarget);
		}
	}

	public static void excelTarget() throws IOException {
		List<String> headers = List.of("string column", "number", "decimal number", "boolean column", "list column", "link column", "some date");
		try (ExcelTarget excelTarget = ExcelTarget.withDefaultsFromFile("/data/testA.xlsx", true, headers)) {
			writeSampleRecords(excelTarget);
		}

		ExcelTargetConfig excelTargetConfig = ExcelTargetConfig.from(FileDataOutputStream.from("/data/testB.xlsx", true));

		// optionally set a type handler on the config to handle Dates, Numbers, Strings, Links, ... differently

		excelTargetConfig.withPrimarySheetName("Results");
		excelTargetConfig.withHeaders(List.of("Title", "Description", "Added"));
		try (ExcelTarget excelTarget = ExcelTarget.withConfig(excelTargetConfig)) {
			excelTarget.writeRow("some title", "some description", new Date());
			excelTarget.writeRow("some title 2", "some description 2", new Date());

			excelTarget.newSheet("Other Stuff", List.of("Id", "Year", "Author"));
			excelTarget.writeRow("1234", 2010, "John Doe");
			excelTarget.writeRow("456", 2011, "Jane Doe");

			excelTarget.newSheet("Even More Stuff");
			excelTarget.appendValue(1);
			excelTarget.appendValue("Lion");
			excelTarget.finishRow();

			excelTarget.appendValue(2);
			excelTarget.appendValue(excelTargetConfig.getBoldHandler(), "Tigers");
			excelTarget.finishRow();

			excelTarget.appendValue(3);
			excelTarget.appendValue("Bears");
			excelTarget.finishRow();
		}
	}

	public static void genericTarget() throws IOException {
		try (SpreadsheetTarget<?, ?> spreadsheetTarget = SpreadsheetTargetFactory.fromFile("/data/test2.xlsx", true)) {
			writeSampleRecords(spreadsheetTarget);
		}

		List<String> standardHeaders = List.of("header1", "header2", "header3", "header4", "header5", "header6", "header7");
		try (SpreadsheetTarget<?, ?> spreadsheetTarget = SpreadsheetTargetFactory.fromFileWithHeaders("/data/test3.xlsx", true, standardHeaders)) {
			writeSampleRecords(spreadsheetTarget);
		}

		try (SpreadsheetTarget<?, ?> spreadsheetTarget = SpreadsheetTargetFactory.fromFileWithHeaders("/data/test3.csv", true, standardHeaders)) {
			writeSampleRecords(spreadsheetTarget);
		}

		try (SpreadsheetTarget<?, ?> spreadsheetTarget = SpreadsheetTargetFactory.fromFileWithHeaders("/data/test3.tsv", true, standardHeaders)) {
			writeSampleRecords(spreadsheetTarget);
		}
	}

	public static void writeSampleRecords(SpreadsheetTarget<?, ?> spreadsheetTarget) {
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

	public static void main(String[] args) throws IOException {
		tsvTarget();
		csvTarget();
		excelTarget();
		genericTarget();
	}
}
