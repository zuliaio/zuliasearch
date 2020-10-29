package io.zulia.client.command.builder;

import io.zulia.message.ZuliaQuery;

public class FilterQuery extends StandardQuery {

	private boolean exclude = false;

	public FilterQuery(String query) {
		super(query);
	}

	public FilterQuery include() {
		exclude = false;
		return this;
	}

	public FilterQuery exclude() {
		exclude = true;
		return this;
	}

	@Override
	protected void completeQuery(ZuliaQuery.Query.Builder queryBuilder) {
		if (exclude) {
			queryBuilder.setQueryType(ZuliaQuery.Query.QueryType.FILTER_NOT);
		}
		else {
			queryBuilder.setQueryType(ZuliaQuery.Query.QueryType.FILTER);
		}
	}

}
