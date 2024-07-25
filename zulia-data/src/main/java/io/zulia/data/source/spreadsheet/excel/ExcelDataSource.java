package io.zulia.data.source.spreadsheet.excel;

import io.zulia.data.common.HeaderMapping;
import io.zulia.data.input.DataInputStream;
import io.zulia.data.source.spreadsheet.SpreadsheetDataSource;
import org.apache.poi.openxml4j.util.ZipInputStreamZipEntrySource;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.util.IOUtils;
import org.apache.poi.util.LocaleUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.SequencedSet;

public class ExcelDataSource implements SpreadsheetDataSource<ExcelDataSourceRecord>, AutoCloseable {

	static {
		LocaleUtil.setUserTimeZone(LocaleUtil.TIMEZONE_UTC);
		IOUtils.setByteArrayMaxOverride(256_000_000);
		ZipInputStreamZipEntrySource.setThresholdBytesForTempFiles(256_000_000);
	}

	private final ExcelDataSourceConfig excelDataSourceConfig;
	private int numberOfRowsForSheet;

	private int currentRow;
	private Workbook reader;
	private Sheet sheet;

	private SheetInfo sheetInfo;

	public static ExcelDataSource withConfig(ExcelDataSourceConfig excelDataSourceConfig) throws IOException {
		return new ExcelDataSource(excelDataSourceConfig);
	}

	public static ExcelDataSource withDefaults(DataInputStream dataInputStream) throws IOException {
		return withConfig(ExcelDataSourceConfig.from(dataInputStream));
	}

	protected ExcelDataSource(ExcelDataSourceConfig excelDataSourceConfig) throws IOException {
		this.excelDataSourceConfig = excelDataSourceConfig;
		open();

		if (Objects.equals(ExcelDataSourceConfig.OpenHandling.ACTIVE_SHEET, excelDataSourceConfig.getOpenHandling())) {
			switchSheet(reader.getActiveSheetIndex());
		}
		else {
			switchSheet(0);
		}

	}

	public void open() throws IOException {
		reader = WorkbookFactory.create(excelDataSourceConfig.getDataInputStream().openInputStream());
	}

	public String getActiveSheetName() {
		return reader.getSheetName(reader.getActiveSheetIndex());
	}

	public int getActiveSheetIndex() {
		return reader.getActiveSheetIndex();
	}

	public int getNumberOfSheets() {
		return reader.getNumberOfSheets();
	}

	public ExcelDataSource switchSheet(int index) {
		sheet = reader.getSheetAt(index);
		initializeSheet();
		return this;
	}

	public ExcelDataSource switchSheet(String name) {
		sheet = reader.getSheet(name);
		initializeSheet();
		return this;
	}

	public boolean hasHeader(String field) {
		if (sheetInfo.headerMapping() == null) {
			throw new IllegalStateException("Cannot get field by name when headers where not read");
		}
		return sheetInfo.headerMapping().hasHeader(field);
	}

	public SequencedSet<String> getHeaders() {
		if (sheetInfo.headerMapping() == null) {
			throw new IllegalStateException("Cannot get headers when headers where not read");
		}
		return sheetInfo.headerMapping().getHeaderKeys();
	}

	private void initializeSheet() {
		numberOfRowsForSheet = sheet.getLastRowNum() + 1;
		Row row = sheet.getRow(0);

		currentRow = 0;
		int numberOfColumns = row.getLastCellNum();

		if (excelDataSourceConfig.hasHeaders()) {
			ExcelCellHandler excelCellHandler = excelDataSourceConfig.getExcelCellHandler();

			List<String> headerRow = new ArrayList<>();
			Row header = sheet.getRow(0);
			if (header != null) {
				for (int i = 0; i < row.getLastCellNum(); i++) {
					Cell cell = row.getCell(i);
					headerRow.add(excelCellHandler.cellToString(cell));
				}
			}
			sheetInfo = new SheetInfo(numberOfColumns, numberOfRowsForSheet, new HeaderMapping(excelDataSourceConfig.getHeaderConfig(), headerRow));
			currentRow++;
		}
		else {
			sheetInfo = new SheetInfo(numberOfColumns, numberOfRowsForSheet, null);
		}

	}

	@Override
	public void reset() throws Exception {
		currentRow = 0;
	}

	public ExcelDataSource setRow(int newRow) {
		currentRow = newRow;
		return this;
	}

	@Override
	public Iterator<ExcelDataSourceRecord> iterator() {

		return new Iterator<>() {

			@Override
			public boolean hasNext() {
				return (currentRow < numberOfRowsForSheet);
			}

			@Override
			public ExcelDataSourceRecord next() {
				Row next = sheet.getRow(currentRow);
				currentRow++;
				return new ExcelDataSourceRecord(next, sheetInfo, excelDataSourceConfig);
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException("Iterator is read only");
			}
		};
	}

	@Override
	public void close() throws Exception {
		reader.close();
	}

}
