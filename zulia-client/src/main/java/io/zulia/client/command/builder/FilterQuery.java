package io.zulia.client.command.builder;

import io.zulia.message.ZuliaQuery;

public class FilterQuery extends StandardQuery {
	public FilterQuery(String query) {
		super(query);
	}

	@Override
	protected void completeQuery(ZuliaQuery.Query.Builder queryBuilder) {
		queryBuilder.setQueryType(ZuliaQuery.Query.QueryType.FILTER);
	}

}
