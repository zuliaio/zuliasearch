package io.zulia.client.command.builder;

import io.zulia.message.ZuliaQuery;

import java.util.Arrays;
import java.util.Collection;

public class NumericSetQuery implements QueryBuilder {

	private final ZuliaQuery.Query.Builder queryBuilder;

	private final ZuliaQuery.NumericSet.Builder numericSetBuilder;

	public NumericSetQuery(String... field) {
		this(Arrays.asList(field));
	}

	public NumericSetQuery(Collection<String> field) {
		if (field.isEmpty()) {
			throw new IllegalArgumentException("Field(s) must be not empty");
		}

		queryBuilder = ZuliaQuery.Query.newBuilder().addAllQf(field).setQueryType(ZuliaQuery.Query.QueryType.NUMERIC_SET);
		numericSetBuilder = ZuliaQuery.NumericSet.newBuilder();

	}

	public NumericSetQuery include() {
		queryBuilder.setQueryType(ZuliaQuery.Query.QueryType.NUMERIC_SET);
		return this;
	}

	public NumericSetQuery exclude() {
		queryBuilder.setQueryType(ZuliaQuery.Query.QueryType.NUMERIC_SET_NOT);
		return this;
	}

	public NumericSetQuery addValue(int value) {
		numericSetBuilder.addIntegerValue(value);
		return this;
	}

	public NumericSetQuery addValue(long value) {
		numericSetBuilder.addLongValue(value);
		return this;
	}

	public NumericSetQuery addValue(float value) {
		numericSetBuilder.addFloatValue(value);
		return this;
	}

	public NumericSetQuery addValue(double value) {
		numericSetBuilder.addDoubleValue(value);
		return this;
	}

	public NumericSetQuery addValues(int... values) {
		for (int v : values) {
			numericSetBuilder.addIntegerValue(v);
		}
		return this;
	}

	public NumericSetQuery addValues(long... values) {
		for (long v : values) {
			numericSetBuilder.addLongValue(v);
		}
		return this;
	}

	public NumericSetQuery addValues(float... values) {
		for (float v : values) {
			numericSetBuilder.addFloatValue(v);
		}
		return this;
	}

	public NumericSetQuery addValues(double... values) {
		for (double v : values) {
			numericSetBuilder.addDoubleValue(v);
		}
		return this;
	}

	public NumericSetQuery addIntValues(Iterable<Integer> values) {
		numericSetBuilder.addAllIntegerValue(values);
		return this;
	}

	public NumericSetQuery addLongValues(Iterable<Long> values) {
		numericSetBuilder.addAllLongValue(values);
		return this;
	}

	public NumericSetQuery addAllFloatValues(Iterable<Float> values) {
		numericSetBuilder.addAllFloatValue(values);
		return this;
	}

	public NumericSetQuery addAllDoubleValues(Iterable<Double> values) {
		numericSetBuilder.addAllDoubleValue(values);
		return this;
	}

	public NumericSetQuery clearValues() {
		numericSetBuilder.clearIntegerValue();
		numericSetBuilder.clearLongValue();
		numericSetBuilder.clearFloatValue();
		numericSetBuilder.clearDoubleValue();
		return this;
	}

	@Override
	public ZuliaQuery.Query getQuery() {
		queryBuilder.setNumericSet(numericSetBuilder);
		return queryBuilder.build();
	}

}
