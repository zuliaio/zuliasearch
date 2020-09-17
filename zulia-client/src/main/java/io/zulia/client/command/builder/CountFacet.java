package io.zulia.client.command.builder;

import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.CountRequest;

public class CountFacet implements CountFacetBuilder {

	private final CountRequest.Builder countRequestBuilder;

	public CountFacet(String field) {
		countRequestBuilder = CountRequest.newBuilder().setFacetField(ZuliaQuery.Facet.newBuilder().setLabel(field).build());
	}

	public CountFacet setTopN(int topN) {
		countRequestBuilder.setMaxFacets(topN);
		return this;
	}

	public CountFacet setTopNShard(int topNShard) {
		countRequestBuilder.setShardFacets(topNShard);
		return this;
	}

	@Override
	public CountRequest getFacetCount() {
		return countRequestBuilder.build();
	}
}
