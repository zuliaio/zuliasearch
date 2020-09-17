package io.zulia.client.command.builder;

import io.zulia.message.ZuliaQuery;

import javax.validation.constraints.NotNull;
import java.util.List;

public abstract class StandardQuery implements QueryBuilder {

	private ZuliaQuery.Query.Builder queryBuilder;

	public StandardQuery(String query) {
		queryBuilder = ZuliaQuery.Query.newBuilder().setQ(query);
	}

	public StandardQuery setQuery(String query) {
		queryBuilder.setQ(query);
		return this;
	}

	public List<String> getQueryFields() {
		return queryBuilder.getQfList();
	}

	public StandardQuery addQueryField(String queryField) {
		queryBuilder.addQf(queryField);
		return this;
	}

	public StandardQuery clearQueryField() {
		queryBuilder.clearQf();
		return this;
	}

	public StandardQuery setQueryFields(@NotNull List<String> queryFields) {
		if (queryFields == null) {
			throw new IllegalArgumentException("Query Fields cannot be null");
		}
		queryBuilder.clearQf();
		queryBuilder.addAllQf(queryFields);
		return this;
	}

	public ZuliaQuery.Query.Operator getDefaultOperator() {
		return queryBuilder.getDefaultOp();
	}

	public StandardQuery setDefaultOperator(ZuliaQuery.Query.Operator defaultOperator) {
		queryBuilder.setDefaultOp(defaultOperator);
		return this;
	}

	public int getMinShouldMatch() {
		return queryBuilder.getMm();
	}

	public StandardQuery setMinShouldMatch(int minShouldMatch) {
		queryBuilder.setMm(minShouldMatch);
		return this;
	}

	public boolean getDismax() {
		return queryBuilder.getDismax();
	}

	public StandardQuery setDismax(boolean dismax) {
		queryBuilder.setDismax(dismax);
		return this;
	}

	public float getDismaxTie() {
		return queryBuilder.getDismaxTie();
	}

	public StandardQuery setDismaxTie(float dismaxTie) {
		queryBuilder.setDismaxTie(dismaxTie);
		return this;
	}

	@Override
	public ZuliaQuery.Query getQuery() {
		completeQuery(queryBuilder);
		return queryBuilder.build();

	}

	protected abstract void completeQuery(ZuliaQuery.Query.Builder queryBuilder);
}
