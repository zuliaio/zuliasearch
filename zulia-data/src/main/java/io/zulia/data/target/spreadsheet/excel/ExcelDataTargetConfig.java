package io.zulia.data.target.spreadsheet.excel;

import io.zulia.data.output.DataOutputStream;
import io.zulia.data.target.spreadsheet.SpreadsheetDataTargetConfig;
import io.zulia.data.target.spreadsheet.excel.cell.*;

public class ExcelDataTargetConfig extends SpreadsheetDataTargetConfig<CellReference, ExcelDataTargetConfig> {
	
	public static ExcelDataTargetConfig from(DataOutputStream dataStream) {
		return new ExcelDataTargetConfig(dataStream);
	}
	
	private String primarySheetName;
	
	private ExcelDataTargetConfig(DataOutputStream dataStream) {
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
	
	public ExcelDataTargetConfig getSelf() {
		return this;
	}
	
	public String getPrimarySheetName() {
		return primarySheetName;
	}
	
	public ExcelDataTargetConfig withPrimarySheetName(String primarySheetName) {
		this.primarySheetName = primarySheetName;
		return this;
	}
}
