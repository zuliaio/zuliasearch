package io.zulia.client.command.builder;

import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.Facet;
import io.zulia.message.ZuliaQuery.Query.Operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DrillDown implements DrillDownBuilder {

	private final String label;
	private List<Facet> facets;
	private Operator operator;
	private int minimumShouldMatch;
	private boolean exclude;

	public DrillDown(String label) {
		this.label = label;
		this.facets = new ArrayList<>();
		this.operator = Operator.OR;
	}

	public DrillDown all() {
		this.operator = Operator.AND;
		return this;
	}

	public DrillDown any() {
		this.operator = Operator.OR;
		return this;
	}

	public DrillDown exclude() {
		this.exclude = true;
		return this;
	}

	public DrillDown include() {
		this.exclude = false;
		return this;
	}

	public DrillDown atLeast(int minimumShouldMatch) {
		this.operator = Operator.OR;
		this.minimumShouldMatch = minimumShouldMatch;
		return this;
	}

	public DrillDown addValues(Iterable<String> values) {
		for (String value : values) {
			addValue(value);
		}
		return this;
	}

	public DrillDown addValues(String... values) {
		for (String value : values) {
			addValue(value);
		}
		return this;
	}

	public DrillDown addValue(String value) {
		return addValue(value, (String[]) null);
	}

	public DrillDown addValue(String value, String... path) {
		Facet.Builder facetBuilder = Facet.newBuilder().setValue(value);
		if (path != null) {
			facetBuilder.addAllPath(Arrays.stream(path).toList());
		}
		return addValue(facetBuilder);
	}

	private DrillDown addValue(Facet.Builder facetBuilder) {
		facets.add(facetBuilder.build());
		return this;
	}

	@Override
	public ZuliaQuery.DrillDown getDrillDown() {
		ZuliaQuery.DrillDown.Builder drillDownBuilder = ZuliaQuery.DrillDown.newBuilder();
		drillDownBuilder.setLabel(label);
		drillDownBuilder.setOperator(operator);
		drillDownBuilder.setMm(minimumShouldMatch);
		drillDownBuilder.addAllFacetValue(facets);
		drillDownBuilder.setExclude(exclude);
		return drillDownBuilder.build();
	}
}
