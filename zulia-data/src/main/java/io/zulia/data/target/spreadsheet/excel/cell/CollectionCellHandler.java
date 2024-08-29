package io.zulia.data.target.spreadsheet.excel.cell;

import io.zulia.data.source.spreadsheet.DelimitedListHandler;
import io.zulia.data.target.spreadsheet.excel.ExcelDataTargetConfig;
import io.zulia.data.target.spreadsheet.excel.WorkbookHelper;
import org.apache.poi.xssf.streaming.SXSSFCell;

import java.util.Collection;

public class CollectionCellHandler implements CellHandler<Collection<?>> {

	public CollectionCellHandler() {

	}

	@Override
	public void handleCell(WorkbookHelper workbookHelper, SXSSFCell cell, Collection<?> collection) {
		if (collection != null) {
			ExcelDataTargetConfig excelDataTargetConfig = workbookHelper.getExcelDataTargetConfig();
			DelimitedListHandler delimitedListHandler = excelDataTargetConfig.getDelimitedListHandler();
			String s = delimitedListHandler.collectionToCellValue(collection);
			excelDataTargetConfig.getStringCellHandler().handleCell(workbookHelper, cell, s);
		}
		else {
			cell.setBlank();
		}
	}
}
