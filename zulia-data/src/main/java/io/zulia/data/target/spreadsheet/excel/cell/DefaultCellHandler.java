package io.zulia.data.target.spreadsheet.excel.cell;

import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;
import io.zulia.data.target.spreadsheet.excel.ExcelTargetConfig;

public class DefaultCellHandler implements SpreadsheetTypeHandler<CellReference, Object> {

	private final ExcelTargetConfig excelDataTargetConfig;

	public DefaultCellHandler(ExcelTargetConfig excelDataTargetConfig) {
		this.excelDataTargetConfig = excelDataTargetConfig;
	}

	@Override
	public void writeType(CellReference reference, Object value) {
		excelDataTargetConfig.getStringTypeHandler().writeType(reference, value != null ? value.toString() : null);
	}
}