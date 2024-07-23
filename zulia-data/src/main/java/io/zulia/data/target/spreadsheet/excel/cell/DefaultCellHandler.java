package io.zulia.data.target.spreadsheet.excel.cell;

import io.zulia.data.target.spreadsheet.excel.WorkbookHelper;
import org.apache.poi.xssf.streaming.SXSSFCell;

public class DefaultCellHandler implements CellHandler<Object> {

	private StringCellHandler stringCellHandler;

	public DefaultCellHandler() {
		this.stringCellHandler = new StringCellHandler();
	}

	@Override
	public void handleCell(WorkbookHelper workbookHelper, SXSSFCell cell, Object object) {
		stringCellHandler.handleCell(workbookHelper, cell, object != null ? object.toString() : null);
	}

}