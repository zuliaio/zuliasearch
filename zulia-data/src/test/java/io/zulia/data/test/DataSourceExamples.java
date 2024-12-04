package io.zulia.data.test;

import io.zulia.data.input.FileDataInputStream;
import io.zulia.data.source.spreadsheet.SpreadsheetRecord;
import io.zulia.data.source.spreadsheet.SpreadsheetSource;
import io.zulia.data.source.spreadsheet.SpreadsheetSourceFactory;
import io.zulia.data.source.spreadsheet.csv.CSVRecord;
import io.zulia.data.source.spreadsheet.csv.CSVSource;
import io.zulia.data.source.spreadsheet.csv.CSVSourceConfig;
import io.zulia.data.source.spreadsheet.excel.DefaultExcelCellHandler;
import io.zulia.data.source.spreadsheet.excel.ExcelRecord;
import io.zulia.data.source.spreadsheet.excel.ExcelSource;
import io.zulia.data.source.spreadsheet.excel.ExcelSourceConfig;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.SequencedSet;

public class DataSourceExamples {

	public static void genericSpreadsheetHandling() throws IOException {

		FileDataInputStream dataInputStream = FileDataInputStream.from("/data/test.csv"); // xls, xlsx and tsv also supported by SpreadsheetSourceFactory
		try (SpreadsheetSource<?> dataSource = SpreadsheetSourceFactory.fromStreamWithHeaders(dataInputStream)) { //reads first line as headers
			// optionally do something with the headers but not required to use headers below
			SequencedSet<String> headers = dataSource.getHeaders();
			
			for (SpreadsheetRecord spreadsheetRecord : dataSource) {
				String firstColumn = spreadsheetRecord.getString(0); // access value in first column not relying on headers
				String title = spreadsheetRecord.getString("title"); // can access by header name because headers were read on open
				Integer year = spreadsheetRecord.getInt("year");
				Float rating = spreadsheetRecord.getFloat("rating");
				Boolean recommended = spreadsheetRecord.getBoolean("recommended");
				Date dateAdded = spreadsheetRecord.getDate("dateAdded");
				List<String> labels = spreadsheetRecord.getList("labels", String.class);
			}
		}

		dataInputStream = FileDataInputStream.from("/data/test.tsv"); // xls, xlsx and csv also supported by SpreadsheetSourceFactory
		try (SpreadsheetSource<?> dataSource = SpreadsheetSourceFactory.fromStreamWithHeaders(dataInputStream)) { //reads first line as headers
			// optionally do something with the headers but not required to use headers below
			SequencedSet<String> headers = dataSource.getHeaders();

			for (SpreadsheetRecord spreadsheetRecord : dataSource) {
				String firstColumn = spreadsheetRecord.getString(0); // access value in first column not relying on headers
				String title = spreadsheetRecord.getString("title"); // can access by header name because headers were read on open
				Integer year = spreadsheetRecord.getInt("year");
				Float rating = spreadsheetRecord.getFloat("rating");
				Boolean recommended = spreadsheetRecord.getBoolean("recommended");
				Date dateAdded = spreadsheetRecord.getDate("dateAdded");
				List<String> labels = spreadsheetRecord.getList("labels", String.class);

			}
		}
		
		try (SpreadsheetSource<?> dataSource = SpreadsheetSourceFactory.fromFileWithHeaders("/data/test.csv")) {  // concise version of above
			
			// two (or more) passes are supported by the iterator
			
			int count = 0;
			for (SpreadsheetRecord spreadsheetRecord : dataSource) {
				count++;
			}
			
			// second more in depth pass using the count for progress for example
			for (SpreadsheetRecord spreadsheetRecord : dataSource) {
			
			}
		}
		
	}

	public static void manualConfigurationOfCSV() throws IOException {
		// manual configuration allows more flexibility than generic by more verbose
		FileDataInputStream dataInputStream = FileDataInputStream.from("/data/test.csv");
		
		CSVSourceConfig csvSourceConfig = CSVSourceConfig.from(dataInputStream);
		csvSourceConfig.withHeaders();
		
		//optionally configure these below
		csvSourceConfig.withDelimiter('|'); // set alternate delimiter
		csvSourceConfig.withListDelimiter(';'); // if reading a cell as a list, split on this, defaults to ;
		csvSourceConfig.withDateParser(s -> {
			// by default dates in format yyyy-mm-dd are supported;
			// implement specialized date parsing here
			return null;
		});
		csvSourceConfig.withBooleanParser(s -> {
			// by default true,t,1,yes,y,false,f,0,no,n are supported
			// implement specialized boolean parsing here
			return null;
		});
		
		try (CSVSource csvSource = CSVSource.withConfig(csvSourceConfig)) {
			for (CSVRecord csvRecord : csvSource) {
				// Standard handling
				String firstColumn = csvRecord.getString(0); // access value in first column not relying on headers
				String title = csvRecord.getString("title"); // can access by header name because headers were read on open
				Integer year = csvRecord.getInt("year");
				Float rating = csvRecord.getFloat("rating");
				Boolean recommended = csvRecord.getBoolean("recommended");
				Date dateAdded = csvRecord.getDate("dateAdded");
				List<String> labels = csvRecord.getList("labels", String.class);
				
				// no special handling for CSV
			}
		}
	}

	public static void manualConfigurationWithExcel() throws IOException {
		FileDataInputStream dataInputStream = FileDataInputStream.from("/data/test.xlsx"); // xlsx and xls are supported;
		ExcelSourceConfig excelSourceConfig = ExcelSourceConfig.from(dataInputStream).withHeaders();
		
		excelSourceConfig.withListDelimiter(';');
		
		// default is DefaultExcelCellHandler but a complete custom implementation can be given or can override individual methods
		excelSourceConfig.withExcelCellHandler(new DefaultExcelCellHandler() {
			@Override
			public Boolean cellToBoolean(Cell cell) {
				// override boolean handling
				return false;
			}
			
			@Override
			public Float cellToFloat(Cell cell) {
				// override boolean handling
				return 0f;
			}
		});
		
		try (ExcelSource excelSource = ExcelSource.withConfig(excelSourceConfig)) {
			
			for (ExcelRecord excelRecord : excelSource) {
				
				// Standard handling
				String firstColumn = excelRecord.getString(0); // access value in first column not relying on headers
				String title = excelRecord.getString("title"); // can access by header name because headers were read on open
				Integer year = excelRecord.getInt("year");
				Float rating = excelRecord.getFloat("rating");
				Boolean recommended = excelRecord.getBoolean("recommended");
				Date dateAdded = excelRecord.getDate("dateAdded");
				List<String> labels = excelRecord.getList("labels", String.class);
				
				//Excel specific
				Row nativeRow = excelRecord.getNativeRow();
				Cell titleCell = excelRecord.getCell("title");
			}
			
		}
	}

	public static void main(String[] args) throws IOException {

	}
	
}
