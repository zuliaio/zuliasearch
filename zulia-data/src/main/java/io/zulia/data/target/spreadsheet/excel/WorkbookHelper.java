package io.zulia.data.target.spreadsheet.excel;

import io.zulia.data.target.spreadsheet.csv.ExcelDataTargetConfig;
import org.apache.poi.common.usermodel.HyperlinkType;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.Hyperlink;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class WorkbookHelper {

	private final Map<String, CellStyle> styleMap = new ConcurrentHashMap<>();
	private final Map<String, Font> fontMap = new ConcurrentHashMap<>();
	private final SXSSFWorkbook workbook;
	private final ExcelDataTargetConfig excelDataTargetConfig;

	public WorkbookHelper(ExcelDataTargetConfig excelDataTargetConfig, SXSSFWorkbook workbook) {
		this.workbook = workbook;
		this.excelDataTargetConfig = excelDataTargetConfig;
	}

	public CellStyle createOrGetStyle(String styleName, Consumer<CellStyle> styleInit) {
		return styleMap.computeIfAbsent(styleName, k -> {
			CellStyle cellStyle = workbook.createCellStyle();
			styleInit.accept(cellStyle);
			return cellStyle;
		});
	}

	public Font createOrGetFont(String fontName, Consumer<Font> fontInit) {
		return fontMap.computeIfAbsent(fontName, k -> {
			Font font = workbook.createFont();
			fontInit.accept(font);
			return font;
		});
	}

	public Hyperlink createLink(String href) {
		Hyperlink link = workbook.getCreationHelper().createHyperlink(HyperlinkType.URL);
		link.setAddress(href);
		return link;
	}

	public ExcelDataTargetConfig getExcelDataTargetConfig() {
		return excelDataTargetConfig;
	}
}
