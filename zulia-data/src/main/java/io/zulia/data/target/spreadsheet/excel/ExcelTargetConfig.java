package io.zulia.data.target.spreadsheet.excel;

import io.zulia.data.output.DataOutputStream;
import io.zulia.data.target.spreadsheet.SpreadsheetTargetConfig;
import io.zulia.data.target.spreadsheet.excel.cell.BoldCellHandler;
import io.zulia.data.target.spreadsheet.excel.cell.BooleanCellHandler;
import io.zulia.data.target.spreadsheet.excel.cell.CellReference;
import io.zulia.data.target.spreadsheet.excel.cell.CollectionCellHandler;
import io.zulia.data.target.spreadsheet.excel.cell.DateCellHandler;
import io.zulia.data.target.spreadsheet.excel.cell.DefaultCellHandler;
import io.zulia.data.target.spreadsheet.excel.cell.HeaderCellHandler;
import io.zulia.data.target.spreadsheet.excel.cell.LinkCellHandler;
import io.zulia.data.target.spreadsheet.excel.cell.NumberCellHandler;
import io.zulia.data.target.spreadsheet.excel.cell.StringCellHandler;

public class ExcelTargetConfig extends SpreadsheetTargetConfig<CellReference, ExcelTargetConfig> {
	
	public static ExcelTargetConfig from(DataOutputStream dataStream) {
		return new ExcelTargetConfig(dataStream);
	}
	
	private String primarySheetName;
	
	private ExcelTargetConfig(DataOutputStream dataStream) {
		super(dataStream);
		withStringHandler(new StringCellHandler());
		withDateTypeHandler(new DateCellHandler());
		withNumberTypeHandler(new NumberCellHandler());
		withLinkTypeHandler(new LinkCellHandler());
		withDefaultTypeHandler(new DefaultCellHandler(this));
		withBooleanTypeHandler(new BooleanCellHandler());
		withCollectionHandler(new CollectionCellHandler(this));
		withHeaderHandler(new HeaderCellHandler());
		withBoldHandler(new BoldCellHandler());
	}
	
	public ExcelTargetConfig getSelf() {
		return this;
	}
	
	public String getPrimarySheetName() {
		return primarySheetName;
	}
	
	public ExcelTargetConfig withPrimarySheetName(String primarySheetName) {
		this.primarySheetName = primarySheetName;
		return this;
	}
}
