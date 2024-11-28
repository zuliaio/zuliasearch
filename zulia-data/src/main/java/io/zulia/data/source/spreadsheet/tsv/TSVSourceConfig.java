package io.zulia.data.source.spreadsheet.tsv;

import io.zulia.data.input.DataInputStream;
import io.zulia.data.source.spreadsheet.delimited.DelimitedSourceConfig;

public class TSVSourceConfig extends DelimitedSourceConfig {
	
	public TSVSourceConfig(DataInputStream dataInputStream) {
		super(dataInputStream);
	}
	
	public static TSVSourceConfig from(DataInputStream dataStream) {
		return new TSVSourceConfig(dataStream);
	}
}
