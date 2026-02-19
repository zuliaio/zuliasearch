package io.zulia.client.command.builder;

import io.zulia.message.ZuliaQuery;

import java.util.List;

public abstract class StandardQuery<T extends StandardQuery<?>> implements QueryBuilder {

	private final ZuliaQuery.Query.Builder queryBuilder;

	public StandardQuery(String query) {

		queryBuilder = ZuliaQuery.Query.newBuilder();

		if (query != null) {
			queryBuilder.setQ(query);
		}
	}

	public T setQuery(String query) {
		queryBuilder.setQ(query);
		return getSelf();
	}

	protected abstract T getSelf();

	public List<String> getQueryFields() {
		return queryBuilder.getQfList();
	}

	public T addQueryField(String queryField) {
		queryBuilder.addQf(queryField);
		return getSelf();
	}

	public T addQueryFields(String... queryFields) {
		queryBuilder.addAllQf(List.of(queryFields));
		return getSelf();
	}

	public T addQueryFields(Iterable<String> queryFields) {
		queryBuilder.addAllQf(queryFields);
		return getSelf();
	}

	public T clearQueryField() {
		queryBuilder.clearQf();
		return getSelf();
	}

	public T setQueryFields(Iterable<String> queryFields) {
		if (queryFields == null) {
			throw new IllegalArgumentException("Query Fields cannot be null");
		}
		queryBuilder.clearQf();
		queryBuilder.addAllQf(queryFields);
		return getSelf();
	}

	public ZuliaQuery.Query.Operator getDefaultOperator() {
		return queryBuilder.getDefaultOp();
	}

	public T setDefaultOperator(ZuliaQuery.Query.Operator defaultOperator) {
		queryBuilder.setDefaultOp(defaultOperator);
		return getSelf();
	}

	public int getMinShouldMatch() {
		return queryBuilder.getMm();
	}

	public T setMinShouldMatch(int minShouldMatch) {
		queryBuilder.setMm(minShouldMatch);
		return getSelf();
	}

	@Override
	public ZuliaQuery.Query getQuery() {
		completeQuery(queryBuilder);
		return queryBuilder.build();

	}

	protected abstract void completeQuery(ZuliaQuery.Query.Builder queryBuilder);

}
