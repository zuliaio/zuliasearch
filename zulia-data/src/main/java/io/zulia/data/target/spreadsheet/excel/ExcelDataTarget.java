package io.zulia.data.target.spreadsheet.excel;

import io.zulia.data.output.DataOutputStream;
import io.zulia.data.output.FileDataOutputStream;
import io.zulia.data.target.spreadsheet.excel.cell.CellHandler;
import io.zulia.data.target.spreadsheet.excel.cell.Link;
import org.apache.poi.xssf.streaming.SXSSFCell;
import org.apache.poi.xssf.streaming.SXSSFRow;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Date;
import java.util.List;

public class ExcelDataTarget implements AutoCloseable {

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
		return withConfig(ExcelDataTargetConfig.from(FileDataOutputStream.from(path, overwrite)).withHeader(headers));
	}

	protected ExcelDataTarget(ExcelDataTargetConfig excelDataTargetConfig) throws IOException {
		this.excelDataTargetConfig = excelDataTargetConfig;
		this.workbook = new SXSSFWorkbook();
		if (excelDataTargetConfig.getPrimarySheetName() != null) {
			this.sheet = this.workbook.createSheet(excelDataTargetConfig.getPrimarySheetName());
		}
		else {
			this.sheet = this.workbook.createSheet();
		}

		this.workbookHelper = new WorkbookHelper(excelDataTargetConfig, workbook);

		Collection<String> headers = excelDataTargetConfig.getHeaders();
		if (headers != null) {
			writeHeaders(headers);
		}
	}

	private void writeHeaders(Collection<String> headers) {
		for (String header : headers) {
			writeNextCell(this.excelDataTargetConfig.getHeaderCellHandler(), header);
		}
		finishRow();
	}

	public void appendValue(Collection<?> value) {
		writeNextCell(excelDataTargetConfig.getCollectionCellHandler(), value);
	}

	public void appendValue(Boolean value) {
		writeNextCell(excelDataTargetConfig.getBooleanCellHandler(), value);
	}

	public void appendValue(Date value) {
		writeNextCell(excelDataTargetConfig.getDateCellHandler(), value);
	}

	public void appendValue(Number value) {
		writeNextCell(excelDataTargetConfig.getNumberCellHandler(), value);
	}

	public void appendValue(String value) {
		writeNextCell(excelDataTargetConfig.getStringCellHandler(), value);
	}

	public void appendValue(Link link) {
		writeNextCell(excelDataTargetConfig.getLinkCellHandler(), link);
	}

	public void appendLink(String label, String href) {
		Link link = new Link(href, label);
		writeNextCell(excelDataTargetConfig.getLinkCellHandler(), link);
	}

	public void appendValue(Object o) {
		if (o instanceof Collection<?> collection) {
			appendValue(collection);
		}
		else if (o instanceof Date date) {
			appendValue(date);
		}
		else if (o instanceof Number number) {
			appendValue(number);
		}
		else if (o instanceof Boolean bool) {
			appendValue(bool);
		}
		else if (o instanceof Link link) {
			appendValue(link);
		}
		else if (o instanceof String string) {
			appendValue(string);
		}
		else {
			writeNextCell(excelDataTargetConfig.getDefaultCellHandler(), o);
		}
	}

	public <T> void writeNextCell(CellHandler<T> cellHandler, T t) {
		SXSSFCell cell = getNextCell();
		cellHandler.handleCell(workbookHelper, cell, t);
	}

	public SXSSFCell getNextCell() {
		if (this.row == null) {
			this.row = this.sheet.createRow(this.rowIdx);
		}

		return this.row.createCell(this.colIdx++);
	}

	public void appendValues(String... values) {
		for (String value : values) {
			appendValue(value);
		}
	}

	public void writeRow(String... values) {
		for (String value : values) {
			appendValue(value);
		}
		finishRow();
	}

	public void writeRow(Object... values) {
		for (Object value : values) {
			appendValue(value);
		}
		finishRow();
	}

	public void writeRow(Collection<?> values) {
		for (Object value : values) {
			appendValue(value);
		}
		finishRow();
	}

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
