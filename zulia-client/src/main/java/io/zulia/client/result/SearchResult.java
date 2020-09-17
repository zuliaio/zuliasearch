package io.zulia.client.result;

import io.zulia.message.ZuliaServiceOuterClass;

public class SearchResult extends QueryResult {
	public SearchResult(ZuliaServiceOuterClass.QueryResponse queryResponse) {
		super(queryResponse);
	}

}
