package io.zulia.data.test;

import io.zulia.data.input.FileDataInputStream;
import io.zulia.data.source.spreadsheet.SpreadsheetDataSource;
import io.zulia.data.source.spreadsheet.SpreadsheetRecord;
import io.zulia.data.source.spreadsheet.SpreadsheetSourceFactory;
import io.zulia.data.source.spreadsheet.csv.CSVDataSource;
import io.zulia.data.source.spreadsheet.csv.CSVDataSourceConfig;
import io.zulia.data.source.spreadsheet.csv.CSVDataSourceRecord;
import io.zulia.data.source.spreadsheet.excel.DefaultExcelCellHandler;
import io.zulia.data.source.spreadsheet.excel.ExcelDataSource;
import io.zulia.data.source.spreadsheet.excel.ExcelDataSourceConfig;
import io.zulia.data.source.spreadsheet.excel.ExcelDataSourceRecord;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.SequencedSet;

public class DataSourceExamples {
	
	public void genericSpreadsheetHandling() throws IOException {
		
		FileDataInputStream dataInputStream = FileDataInputStream.from("/data/test.csv"); // xls, csv and tsv also supported by SpreadsheetSourceFactory
		try (SpreadsheetDataSource<?> dataSource = SpreadsheetSourceFactory.fromStreamWithHeaders(dataInputStream)) { //reads first line as headers
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
		
		try (SpreadsheetDataSource<?> dataSource = SpreadsheetSourceFactory.fromFileWithHeaders("/data/test.csv")) {  // concise version of above
			
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
	
	public void manualConfigurationOfCSV() throws IOException {
		// manual configuration allows more flexibility than generic by more verbose
		FileDataInputStream dataInputStream = FileDataInputStream.from("/data/test.tsv");
		
		CSVDataSourceConfig csvDataSourceConfig = CSVDataSourceConfig.from(dataInputStream).withHeaders();
		
		//optionally configure these below
		csvDataSourceConfig.withDelimiter('\t'); // set delimiter
		csvDataSourceConfig.withListDelimiter(';'); // if reading a cell as a list, split on this, defaults to ;
		csvDataSourceConfig.withDateParser(s -> {
			// by default dates in format yyyy-mm-dd are supported;
			// implement specialized date parsing here
			return null;
		});
		csvDataSourceConfig.withBooleanParser(s -> {
			// by default true,t,1,yes,y,false,f,0,no,n are supported
			// implement specialized boolean parsing here
			return null;
		});
		
		try (CSVDataSource csvDataSource = CSVDataSource.withConfig(csvDataSourceConfig)) {
			for (CSVDataSourceRecord csvDataSourceRecord : csvDataSource) {
				// Standard handling
				String firstColumn = csvDataSourceRecord.getString(0); // access value in first column not relying on headers
				String title = csvDataSourceRecord.getString("title"); // can access by header name because headers were read on open
				Integer year = csvDataSourceRecord.getInt("year");
				Float rating = csvDataSourceRecord.getFloat("rating");
				Boolean recommended = csvDataSourceRecord.getBoolean("recommended");
				Date dateAdded = csvDataSourceRecord.getDate("dateAdded");
				List<String> labels = csvDataSourceRecord.getList("labels", String.class);
				
				// no special handling for CSV
			}
		}
	}
	
	public void manualConfigurationWithExcel() throws IOException {
		FileDataInputStream dataInputStream = FileDataInputStream.from("/data/test.xlsx"); // xlsx and xls are supported;
		ExcelDataSourceConfig excelDataSourceConfig = ExcelDataSourceConfig.from(dataInputStream).withHeaders();
		
		excelDataSourceConfig.withListDelimiter(';');
		
		// default is DefaultExcelCellHandler but a complete custom implementation can be given or can override individual methods
		excelDataSourceConfig.withExcelCellHandler(new DefaultExcelCellHandler() {
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
		
		try (ExcelDataSource dataSource = ExcelDataSource.withConfig(excelDataSourceConfig)) {
			
			for (ExcelDataSourceRecord excelDataSourceRecord : dataSource) {
				
				// Standard handling
				String firstColumn = excelDataSourceRecord.getString(0); // access value in first column not relying on headers
				String title = excelDataSourceRecord.getString("title"); // can access by header name because headers were read on open
				Integer year = excelDataSourceRecord.getInt("year");
				Float rating = excelDataSourceRecord.getFloat("rating");
				Boolean recommended = excelDataSourceRecord.getBoolean("recommended");
				Date dateAdded = excelDataSourceRecord.getDate("dateAdded");
				List<String> labels = excelDataSourceRecord.getList("labels", String.class);
				
				//Excel specific
				Row nativeRow = excelDataSourceRecord.getNativeRow();
				Cell titleCell = excelDataSourceRecord.getCell("title");
			}
			
		}
	}
	
}
