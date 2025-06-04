package io.zulia.client.rest.options;

import io.zulia.message.ZuliaQuery.Query.Operator;

import java.util.Collection;
import java.util.List;

public class SearchREST {

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
	private Boolean realtime;

	public SearchREST(String index) {
		this.indexNames = List.of(index);
	}

	public SearchREST(String... indexNames) {
		setIndexNames(indexNames);
	}

	public SearchREST(Collection<String> indexNames) {
		setIndexNames(indexNames);
	}

	public Collection<String> getIndexNames() {
		return indexNames;
	}

	public SearchREST setIndexNames(String... indexNames) {
		return setIndexNames(List.of(indexNames));
	}

	public SearchREST setIndexNames(Collection<String> indexNames) {
		this.indexNames = indexNames;
		return this;
	}

	public String getQuery() {
		return query;
	}

	public SearchREST setQuery(String query) {
		this.query = query;
		return this;
	}

	public Collection<String> getQueryFields() {
		return queryFields;
	}

	public SearchREST setQueryFields(String... queryFields) {
		return setQueryFields(List.of(queryFields));
	}

	public SearchREST setQueryFields(List<String> queryFields) {
		this.queryFields = queryFields;
		return this;
	}

	public Collection<String> getFilterQueries() {
		return filterQueries;
	}

	public SearchREST setFilterQueries(String... filterQueries) {
		return setFilterQueries(List.of(filterQueries));
	}

	public SearchREST setFilterQueries(List<String> filterQueries) {
		this.filterQueries = filterQueries;
		return this;
	}

	public Collection<String> getFields() {
		return fields;
	}

	public SearchREST setFields(String... fields) {
		return setFields(List.of(fields));
	}

	public SearchREST setFields(List<String> fields) {

		this.fields = fields;
		return this;
	}

	public int getRows() {
		return rows;
	}

	public SearchREST setRows(int rows) {
		this.rows = rows;
		return this;
	}

	public Collection<String> getFacet() {
		return facet;
	}

	public SearchREST setFacet(String... facet) {
		return setFacet(List.of(facet));
	}

	public SearchREST setFacet(List<String> facet) {
		this.facet = facet;
		return this;
	}

	public Collection<String> getDrillDowns() {
		return drillDowns;
	}

	public SearchREST setDrillDowns(String... drillDowns) {
		return setDrillDowns(List.of(drillDowns));
	}

	public SearchREST setDrillDowns(List<String> drillDowns) {
		this.drillDowns = drillDowns;
		return this;
	}

	public Operator getDefaultOperator() {
		return defaultOperator;
	}

	public Integer getMm() {
		return mm;
	}

	public SearchREST setMm(Integer mm) {
		this.mm = mm;
		return this;
	}

	public Collection<String> getSort() {
		return sort;
	}

	public SearchREST setSort(String... sort) {
		return setSort(List.of(sort));
	}

	public SearchREST setSort(List<String> sort) {
		this.sort = sort;
		return this;
	}

	public Collection<String> getHighlights() {
		return highlights;
	}

	public SearchREST setHighlights(String... highlights) {
		return setHighlights(List.of(highlights));
	}

	public SearchREST setHighlights(List<String> highlights) {
		this.highlights = highlights;
		return this;
	}

	public String getCursor() {
		return cursor;
	}

	public SearchREST setCursor(String cursor) {
		this.cursor = cursor;
		return this;
	}

	public SearchREST setDefaultOperator(Operator defaultOperator) {
		this.defaultOperator = defaultOperator;
		return this;
	}

	public SearchREST setRealtime(Boolean realtime) {
		this.realtime = realtime;
		return this;
	}

	public Boolean isRealtime() {
		return realtime;
	}

	@Override
	public String toString() {
		return "SearchREST{" + "indexNames=" + indexNames + ", query='" + query + '\'' + ", queryFields=" + queryFields + ", filterQueries=" + filterQueries
				+ ", fields=" + fields + ", rows=" + rows + ", facet=" + facet + ", drillDowns=" + drillDowns + ", defaultOperator=" + defaultOperator + ", mm="
				+ mm + ", sort=" + sort + ", highlights=" + highlights + ", cursor='" + cursor + '\'' + ", realtime=" + realtime + '}';
	}
}
