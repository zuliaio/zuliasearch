package io.zulia.client.command.factory;

import io.zulia.util.QueryHelper;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

public class NumericSet {
	
	private final Collection<String> fields;
	private Collection<Number> values;
	private boolean exclude;
	
	private Function<Number, String> numberFormatter = Number::toString;
	
	public static NumericSet defaultFields() {
		return new NumericSet(List.of());
	}
	
	public static NumericSet withField(String field) {
		return new NumericSet(List.of(field));
	}
	
	public static NumericSet withFields(String... fields) {
		return new NumericSet(List.of(fields));
	}
	
	public static NumericSet withFields(Collection<String> fields) {
		return new NumericSet(fields);
	}
	
	private NumericSet(Collection<String> fields) {
		this.fields = fields;
	}
	
	public NumericSet exclude() {
		this.exclude = true;
		return this;
	}
	
	public NumericSet include() {
		this.exclude = false;
		return this;
	}
	
	public NumericSet numberFormatter(Function<Number, String> numberFormatter) {
		this.numberFormatter = numberFormatter;
		return this;
	}
	
	public NumericSet of(Number... values) {
		this.values = Arrays.stream(values).toList();
		return this;
	}
	
	public NumericSet of(Collection<Number> values) {
		this.values = values;
		return this;
	}
	
	
	public String asString() {
		
		StringBuilder sb = new StringBuilder();
		
		if (exclude) {
			sb.append("-");
		}
		
		if (fields != null && !fields.isEmpty()) {
			QueryHelper.COMMA_JOINER.appendTo(sb, fields);
			sb.append(":");
		}
		sb.append("zl:ns");
		sb.append("(");
		
		List<String> valuesHandled = values.stream().map(numberFormatter).toList();
		QueryHelper.SPACE_JOINER.appendTo(sb, valuesHandled);
		sb.append(")");
		
		return sb.toString();
	}
}
