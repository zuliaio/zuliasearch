package io.zulia.client.rest.options;

import io.zulia.message.ZuliaQuery.Query.Operator;

import java.util.Collection;
import java.util.List;

public class SearchRest {

	private Collection<String> indexNames;

	private String query;

	private Collection<String> queryFields;

	private Collection<String> filterQueries;

	private Collection<String> fields;

	private int rows;

	private Collection<String> facet;

	private Collection<String> drillDowns;

	private Operator defaultOperator;

	private Integer mm;

	private Collection<String> sort;

	private Collection<String> highlights;

	private String cursor;

	public SearchRest(String index) {
		this.indexNames = List.of(index);
	}

	public SearchRest(String... indexNames) {
		setIndexNames(indexNames);
	}

	public SearchRest(Collection<String> indexNames) {
		setIndexNames(indexNames);
	}

	public Collection<String> getIndexNames() {
		return indexNames;
	}

	public SearchRest setIndexNames(String... indexNames) {
		return setIndexNames(List.of(indexNames));
	}

	public SearchRest setIndexNames(Collection<String> indexNames) {
		this.indexNames = indexNames;
		return this;
	}

	public String getQuery() {
		return query;
	}

	public SearchRest setQuery(String query) {
		this.query = query;
		return this;
	}

	public Collection<String> getQueryFields() {
		return queryFields;
	}

	public SearchRest setQueryFields(String... queryFields) {
		return setQueryFields(List.of(queryFields));
	}

	public SearchRest setQueryFields(List<String> queryFields) {
		this.queryFields = queryFields;
		return this;
	}

	public Collection<String> getFilterQueries() {
		return filterQueries;
	}

	public SearchRest setFilterQueries(String... filterQueries) {
		return setFilterQueries(List.of(filterQueries));
	}

	public SearchRest setFilterQueries(List<String> filterQueries) {
		this.filterQueries = filterQueries;
		return this;
	}

	public Collection<String> getFields() {
		return fields;
	}

	public SearchRest setFields(String... fields) {
		return setFields(List.of(fields));
	}

	public SearchRest setFields(List<String> fields) {

		this.fields = fields;
		return this;
	}

	public int getRows() {
		return rows;
	}

	public SearchRest setRows(int rows) {
		this.rows = rows;
		return this;
	}

	public Collection<String> getFacet() {
		return facet;
	}

	public SearchRest setFacet(String... facet) {
		return setFacet(List.of(facet));
	}

	public SearchRest setFacet(List<String> facet) {
		this.facet = facet;
		return this;
	}

	public Collection<String> getDrillDowns() {
		return drillDowns;
	}

	public SearchRest setDrillDowns(String... drillDowns) {
		return setDrillDowns(List.of(drillDowns));
	}

	public SearchRest setDrillDowns(List<String> drillDowns) {
		this.drillDowns = drillDowns;
		return this;
	}

	public Operator getDefaultOperator() {
		return defaultOperator;
	}

	public Integer getMm() {
		return mm;
	}

	public SearchRest setMm(Integer mm) {
		this.mm = mm;
		return this;
	}

	public Collection<String> getSort() {
		return sort;
	}

	public SearchRest setSort(String... sort) {
		return setSort(List.of(sort));
	}

	public SearchRest setSort(List<String> sort) {
		this.sort = sort;
		return this;
	}

	public Collection<String> getHighlights() {
		return highlights;
	}

	public SearchRest setHighlights(String... highlights) {
		return setHighlights(List.of(highlights));
	}

	public SearchRest setHighlights(List<String> highlights) {
		this.highlights = highlights;
		return this;
	}

	public String getCursor() {
		return cursor;
	}

	public SearchRest setCursor(String cursor) {
		this.cursor = cursor;
		return this;
	}

	public SearchRest setDefaultOperator(Operator defaultOperator) {
		this.defaultOperator = defaultOperator;
		return this;
	}

	@Override
	public String toString() {
		return "SearchRest{" + "indexNames=" + indexNames + ", query='" + query + '\'' + ", queryFields=" + queryFields + ", filterQueries=" + filterQueries
				+ ", fields=" + fields + ", rows=" + rows + ", facet=" + facet + ", drillDowns=" + drillDowns + ", defaultOperator='" + defaultOperator + '\''
				+ ", mm=" + mm + ", sort=" + sort + ", highlights=" + highlights + ", cursor='" + cursor + '\'' + '}';
	}
}
