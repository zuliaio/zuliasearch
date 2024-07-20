package io.zulia.data.target.spreadsheet.excel.cell;

import io.zulia.data.target.spreadsheet.excel.WorkbookHelper;
import org.apache.poi.xssf.streaming.SXSSFCell;

public interface CellHandler<T> {

	void handleCell(WorkbookHelper workbookHelper, SXSSFCell cell, T t);
}
