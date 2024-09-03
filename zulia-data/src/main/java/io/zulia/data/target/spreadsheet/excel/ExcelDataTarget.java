package io.zulia.data.target.spreadsheet.excel;

import io.zulia.data.output.DataOutputStream;
import io.zulia.data.output.FileDataOutputStream;
import io.zulia.data.target.spreadsheet.SpreadsheetDataTarget;
import io.zulia.data.target.spreadsheet.excel.cell.CellReference;
import org.apache.poi.xssf.streaming.SXSSFCell;
import org.apache.poi.xssf.streaming.SXSSFRow;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;

public class ExcelDataTarget extends SpreadsheetDataTarget<CellReference, ExcelDataTargetConfig> {
	
	private final ExcelDataTargetConfig excelDataTargetConfig;
	private final SXSSFWorkbook workbook;
	private final WorkbookHelper workbookHelper;
	
	private SXSSFSheet sheet;
	private SXSSFRow row;
	private int rowIdx = 0;
	private int colIdx = 0;
	
	public static ExcelDataTarget withConfig(ExcelDataTargetConfig excelDataTargetConfig) throws IOException {
		return new ExcelDataTarget(excelDataTargetConfig);
	}
	
	public static ExcelDataTarget withDefaults(DataOutputStream dataOutputStream) throws IOException {
		return withConfig(ExcelDataTargetConfig.from(dataOutputStream));
	}
	
	public static ExcelDataTarget withDefaultsFromFile(String path, boolean overwrite) throws IOException {
		return withDefaults(FileDataOutputStream.from(path, overwrite));
	}
	
	public static ExcelDataTarget withDefaultsFromFile(String path, boolean overwrite, Collection<String> headers) throws IOException {
		return withConfig(ExcelDataTargetConfig.from(FileDataOutputStream.from(path, overwrite)).withHeaders(headers));
	}
	
	protected ExcelDataTarget(ExcelDataTargetConfig excelDataTargetConfig) throws IOException {
		super(excelDataTargetConfig);
		this.excelDataTargetConfig = excelDataTargetConfig;
		this.workbook = new SXSSFWorkbook();
		if (excelDataTargetConfig.getPrimarySheetName() != null) {
			this.sheet = this.workbook.createSheet(excelDataTargetConfig.getPrimarySheetName());
		}
		else {
			this.sheet = this.workbook.createSheet();
		}
		
		this.workbookHelper = new WorkbookHelper(workbook);
		
		Collection<String> headers = excelDataTargetConfig.getHeaders();
		if (headers != null) {
			writeHeaders(headers);
		}
	}
	
	protected CellReference generateReference() {
		SXSSFCell cell = getNextCell();
		return new CellReference(workbookHelper, cell);
	}
	
	public SXSSFCell getNextCell() {
		if (this.row == null) {
			this.row = this.sheet.createRow(this.rowIdx);
		}
		
		return this.row.createCell(this.colIdx++);
	}
	
	@Override
	public void finishRow() {
		rowIdx++;
		colIdx = 0;
		row = null;
	}
	
	public void newSheet(String sheetName) {
		newSheet(sheetName, null);
	}
	
	public void newSheet(String sheetName, List<String> headers) {
		this.sheet = this.workbook.createSheet(sheetName);
		this.row = null;
		this.rowIdx = 0;
		this.colIdx = 0;
		if (headers != null) {
			writeHeaders(headers);
		}
		
	}
	
	public void close() throws IOException {
		try (OutputStream stream = excelDataTargetConfig.getDataStream().openOutputStream()) {
			this.workbook.write(stream);
		}
	}
	
}
