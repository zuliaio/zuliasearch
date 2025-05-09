package io.zulia.data.source.spreadsheet.excel;

import io.zulia.data.common.HeaderMapping;
import io.zulia.data.input.DataInputStream;
import io.zulia.data.source.spreadsheet.SpreadsheetSource;
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

public class ExcelSource implements SpreadsheetSource<ExcelRecord>, AutoCloseable {

	static {
		LocaleUtil.setUserTimeZone(LocaleUtil.TIMEZONE_UTC);
		IOUtils.setByteArrayMaxOverride(256_000_000);
		ZipInputStreamZipEntrySource.setThresholdBytesForTempFiles(256_000_000);
	}

	private final ExcelSourceConfig excelSourceConfig;
	private int numberOfRowsForSheet;

	private int currentRow;
	private Workbook reader;
	private Sheet sheet;

	private SheetInfo sheetInfo;

	public static ExcelSource withConfig(ExcelSourceConfig excelSourceConfig) throws IOException {
		return new ExcelSource(excelSourceConfig);
	}

	public static ExcelSource withDefaults(DataInputStream dataInputStream) throws IOException {
		return withConfig(ExcelSourceConfig.from(dataInputStream));
	}

	protected ExcelSource(ExcelSourceConfig excelSourceConfig) throws IOException {
		this.excelSourceConfig = excelSourceConfig;
		open();

		if (Objects.equals(ExcelSourceConfig.OpenHandling.ACTIVE_SHEET, excelSourceConfig.getOpenHandling())) {
			switchSheet(reader.getActiveSheetIndex());
		}
		else {
			switchSheet(0);
		}

	}

	public void open() throws IOException {
		reader = WorkbookFactory.create(excelSourceConfig.getDataInputStream().openInputStream());
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

	public ExcelSource switchSheet(int index) {
		sheet = reader.getSheetAt(index);
		initializeSheet();
		return this;
	}

	public ExcelSource switchSheet(String name) {
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

		int numberOfColumns = row.getLastCellNum();

		if (excelSourceConfig.hasHeaders()) {
			ExcelCellHandler excelCellHandler = excelSourceConfig.getExcelCellHandler();

			List<String> headerRow = new ArrayList<>();
			Row header = sheet.getRow(0);
			if (header != null) {
				for (int i = 0; i < row.getLastCellNum(); i++) {
					Cell cell = row.getCell(i);
					headerRow.add(excelCellHandler.cellToString(cell));
				}
			}
			sheetInfo = new SheetInfo(numberOfColumns, numberOfRowsForSheet, new HeaderMapping(excelSourceConfig.getHeaderConfig(), headerRow));
		}
		else {
			sheetInfo = new SheetInfo(numberOfColumns, numberOfRowsForSheet, null);
		}
		currentRow = getStartRow();

	}

	@Override
	public void reset() {
		currentRow = getStartRow();
	}

	private int getStartRow() {
		return excelSourceConfig.hasHeaders() ? 1 : 0;
	}

	public ExcelSource setRow(int newRow) {
		currentRow = newRow;
		return this;
	}

	@Override
	public Iterator<ExcelRecord> iterator() {

		if (currentRow != getStartRow()) {
			reset();
		}

		return new Iterator<>() {

			@Override
			public boolean hasNext() {
				return (currentRow < numberOfRowsForSheet);
			}

			@Override
			public ExcelRecord next() {
				Row next = sheet.getRow(currentRow);
				currentRow++;
				return new ExcelRecord(next, sheetInfo, excelSourceConfig);
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException("Iterator is read only");
			}
		};
	}

	@Override
	public void close() throws IOException {
		reader.close();
	}

}
