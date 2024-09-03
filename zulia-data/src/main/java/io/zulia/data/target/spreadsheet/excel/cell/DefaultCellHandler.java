package io.zulia.data.target.spreadsheet.excel.cell;

import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;
import io.zulia.data.target.spreadsheet.excel.ExcelDataTargetConfig;

public class DefaultCellHandler implements SpreadsheetTypeHandler<CellReference, Object> {
	
	private final ExcelDataTargetConfig excelDataTargetConfig;
	
	public DefaultCellHandler(ExcelDataTargetConfig excelDataTargetConfig) {
		this.excelDataTargetConfig = excelDataTargetConfig;
	}
	
	@Override
	public void writeType(CellReference reference, Object value) {
		excelDataTargetConfig.getStringTypeHandler().writeType(reference, value != null ? value.toString() : null);
	}
}