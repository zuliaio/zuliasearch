package io.zulia.data.target.spreadsheet.delimited;

import com.univocity.parsers.common.AbstractWriter;
import io.zulia.data.output.DataOutputStream;
import io.zulia.data.target.spreadsheet.SpreadsheetTargetConfig;
import io.zulia.data.target.spreadsheet.delimited.formatter.BooleanDelimitedWriter;
import io.zulia.data.target.spreadsheet.delimited.formatter.CollectionDelimitedWriter;
import io.zulia.data.target.spreadsheet.delimited.formatter.DateCSVWriter;
import io.zulia.data.target.spreadsheet.delimited.formatter.DefaultCSVWriter;
import io.zulia.data.target.spreadsheet.delimited.formatter.LinkCSVWriter;
import io.zulia.data.target.spreadsheet.delimited.formatter.NumberCSVWriter;
import io.zulia.data.target.spreadsheet.delimited.formatter.StringDelimitedWriter;

public abstract class DelimitedTargetConfig<T extends AbstractWriter<?>, S extends DelimitedTargetConfig<T, S>> extends SpreadsheetTargetConfig<T, S> {

	public DelimitedTargetConfig(DataOutputStream dataStream) {
		super(dataStream);
		withStringHandler(new StringDelimitedWriter<>());
		withDateTypeHandler(new DateCSVWriter<>());
		withNumberTypeHandler(new NumberCSVWriter<>());
		withLinkTypeHandler(new LinkCSVWriter<>());
		withDefaultTypeHandler(new DefaultCSVWriter<>());
		withBooleanTypeHandler(new BooleanDelimitedWriter<>());
		withCollectionHandler(new CollectionDelimitedWriter<>(this));
		withHeaderHandler(new StringDelimitedWriter<>()); // csv headers are just strings
	}

}
