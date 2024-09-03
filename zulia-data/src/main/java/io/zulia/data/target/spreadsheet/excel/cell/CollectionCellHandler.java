package io.zulia.data.target.spreadsheet.excel.cell;

import io.zulia.data.source.spreadsheet.DelimitedListHandler;
import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;
import io.zulia.data.target.spreadsheet.excel.ExcelDataTargetConfig;

import java.util.Collection;

public class CollectionCellHandler implements SpreadsheetTypeHandler<CellReference, Collection<?>> {
	
	private final ExcelDataTargetConfig excelDataTargetConfig;
	
	public CollectionCellHandler(ExcelDataTargetConfig excelDataTargetConfig) {
		this.excelDataTargetConfig = excelDataTargetConfig;
	}
	
	@Override
	public void writeType(CellReference reference, Collection<?> value) {
		if (value != null) {
			;
			DelimitedListHandler delimitedListHandler = excelDataTargetConfig.getDelimitedListHandler();
			String s = delimitedListHandler.collectionToCellValue(value);
			excelDataTargetConfig.getStringTypeHandler().writeType(reference, s);
		}
		else {
			reference.cell().setBlank();
		}
	}
}
