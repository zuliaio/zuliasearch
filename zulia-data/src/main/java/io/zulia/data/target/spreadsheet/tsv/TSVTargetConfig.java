package io.zulia.data.target.spreadsheet.tsv;

import io.zulia.data.output.DataOutputStream;
import io.zulia.data.target.spreadsheet.delimited.DelimitedTargetConfig;

import java.util.List;

public class TSVTargetConfig extends DelimitedTargetConfig<List<String>, TSVTargetConfig> {

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
