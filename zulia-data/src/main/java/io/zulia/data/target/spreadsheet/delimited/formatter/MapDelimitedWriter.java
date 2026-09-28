package io.zulia.data.target.spreadsheet.delimited.formatter;

import io.zulia.data.target.spreadsheet.MapJson;
import io.zulia.data.target.spreadsheet.SpreadsheetTypeHandler;
import io.zulia.data.target.spreadsheet.delimited.DelimitedTargetConfig;

import java.util.List;
import java.util.Map;

public class MapDelimitedWriter<T extends List<String>, S extends DelimitedTargetConfig<T, S>> implements SpreadsheetTypeHandler<T, Map<?, ?>> {

	private final DelimitedTargetConfig<T, S> csvDataTargetConfig;

	public MapDelimitedWriter(DelimitedTargetConfig<T, S> csvDataTargetConfig) {
		this.csvDataTargetConfig = csvDataTargetConfig;
	}

	@Override
	public void writeType(T reference, Map<?, ?> value) {
		csvDataTargetConfig.getStringTypeHandler().writeType(reference, value != null ? MapJson.toJson(value) : null);
	}
}
