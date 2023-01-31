package io.zulia.client.command.builder;

import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.CountRequest;

import java.util.Arrays;

public class CountFacet implements CountFacetBuilder {

	private final CountRequest.Builder countRequestBuilder;

	public CountFacet(String field) {
		countRequestBuilder = CountRequest.newBuilder().setFacetField(ZuliaQuery.Facet.newBuilder().setLabel(field).build());
	}

	public CountFacet(String field, String... path) {
		this(field, Arrays.asList(path));
	}

	public CountFacet(String field, Iterable<String> path) {
		countRequestBuilder = CountRequest.newBuilder().setFacetField(ZuliaQuery.Facet.newBuilder().setLabel(field).addAllPath(path).build());
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
