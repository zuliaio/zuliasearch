package io.zulia.data.target.spreadsheet.tsv;

import com.univocity.parsers.tsv.TsvWriter;
import com.univocity.parsers.tsv.TsvWriterSettings;
import io.zulia.data.output.DataOutputStream;
import io.zulia.data.output.FileDataOutputStream;
import io.zulia.data.target.spreadsheet.delimited.DelimitedTarget;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

public class TSVTarget extends DelimitedTarget<TsvWriter, TSVTargetConfig> {
	
	private final TSVTargetConfig tsvTargetConfig;
	private TsvWriterSettings settings;
	
	public static TSVTarget withConfig(TSVTargetConfig csvDataTargetConfig) throws IOException {
		return new TSVTarget(csvDataTargetConfig);
	}
	
	public static TSVTarget withDefaults(DataOutputStream dataOutputStream) throws IOException {
		return withConfig(TSVTargetConfig.from(dataOutputStream));
	}
	
	public static TSVTarget withDefaultsFromFile(String path, boolean overwrite) throws IOException {
		return withDefaults(FileDataOutputStream.from(path, overwrite));
	}
	
	public static TSVTarget withDefaultsFromFile(String path, boolean overwrite, Collection<String> headers) throws IOException {
		return withConfig(TSVTargetConfig.from(FileDataOutputStream.from(path, overwrite)).withHeaders(headers));
	}
	
	protected TSVTarget(TSVTargetConfig tsvTargetConfig) throws IOException {
		super(tsvTargetConfig);
		this.tsvTargetConfig = tsvTargetConfig;
	}
	
	@Override
	protected void init(TSVTargetConfig delimitedTargetConfig) {
		settings = new TsvWriterSettings();
		settings.setMaxColumns(2048);
	}
	
	@Override
	protected TsvWriter createWriter(OutputStream outputStream) throws IOException {
		return new TsvWriter(outputStream, settings);
	}
	
}
