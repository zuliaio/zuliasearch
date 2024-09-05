package io.zulia.data.target.spreadsheet.excel;

import io.zulia.data.output.DataOutputStream;
import io.zulia.data.target.spreadsheet.SpreadsheetTargetConfig;
import io.zulia.data.target.spreadsheet.excel.cell.*;

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
