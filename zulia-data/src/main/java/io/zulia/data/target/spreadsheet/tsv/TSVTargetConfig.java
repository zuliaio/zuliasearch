package io.zulia.data.target.spreadsheet.tsv;

import com.univocity.parsers.tsv.TsvWriter;
import io.zulia.data.output.DataOutputStream;
import io.zulia.data.target.spreadsheet.delimited.DelimitedTargetConfig;

public class TSVTargetConfig extends DelimitedTargetConfig<TsvWriter, TSVTargetConfig> {
	
	public static TSVTargetConfig from(DataOutputStream dataStream) {
		return new TSVTargetConfig(dataStream);
	}
	
	public TSVTargetConfig(DataOutputStream dataStream) {
		super(dataStream);
		
	}
	
	@Override
	protected TSVTargetConfig getSelf() {
		return this;
	}
	
}
