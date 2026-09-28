package io.zulia.data.target.spreadsheet.excel.cell;

import io.zulia.data.target.spreadsheet.MapJson;
import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;
import io.zulia.data.target.spreadsheet.excel.ExcelTargetConfig;

import java.util.Map;

public class MapCellHandler implements SpreadsheetTypeHandler<CellReference, Map<?, ?>> {

	private final ExcelTargetConfig excelDataTargetConfig;

	public MapCellHandler(ExcelTargetConfig excelDataTargetConfig) {
		this.excelDataTargetConfig = excelDataTargetConfig;
	}

	@Override
	public void writeType(CellReference reference, Map<?, ?> value) {
		excelDataTargetConfig.getStringTypeHandler().writeType(reference, value != null ? MapJson.toJson(value) : null);
	}
}
