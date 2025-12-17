package io.zulia.data.target.spreadsheet.tsv;

import de.siegmar.fastcsv.writer.CsvWriter;
import io.zulia.data.output.DataOutputStream;
import io.zulia.data.output.FileDataOutputStream;
import io.zulia.data.target.spreadsheet.delimited.DelimitedTarget;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;

public class TSVTarget extends DelimitedTarget<TSVTargetConfig> {

	private CsvWriter.CsvWriterBuilder builder;

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
	}

	@Override
	protected void init(TSVTargetConfig tsvTargetConfig) {
		builder = CsvWriter.builder().autoFlush(true).fieldSeparator('\t');
	}

	@Override
	protected CsvWriter createWriter(OutputStream outputStream) {
		return builder.build(outputStream);
	}

}
