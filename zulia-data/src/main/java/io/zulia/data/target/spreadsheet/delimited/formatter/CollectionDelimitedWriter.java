package io.zulia.data.target.spreadsheet.delimited.formatter;

import com.univocity.parsers.common.AbstractWriter;
import io.zulia.data.source.spreadsheet.DelimitedListHandler;
import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;
import io.zulia.data.target.spreadsheet.delimited.DelimitedTargetConfig;

import java.util.Collection;

public class CollectionDelimitedWriter<T extends AbstractWriter<?>, S extends DelimitedTargetConfig<T, S>> implements SpreadsheetTypeHandler<T, Collection<?>> {

	private final DelimitedTargetConfig<T, S> csvDataTargetConfig;

	public CollectionDelimitedWriter(DelimitedTargetConfig<T, S> csvDataTargetConfig) {
		this.csvDataTargetConfig = csvDataTargetConfig;
	}

	@Override
	public void writeType(T reference, Collection<?> value) {
		if (value != null) {
			DelimitedListHandler delimitedListHandler = csvDataTargetConfig.getDelimitedListHandler();
			String s = delimitedListHandler.collectionToCellValue(value);
			SpreadsheetTypeHandler<T, String> stringCellHandler = csvDataTargetConfig.getStringTypeHandler();
			stringCellHandler.writeType(reference, s);
		}
		else {
			reference.addValue(null);
		}
	}
}
