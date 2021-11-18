package io.zulia.client.command.builder;

import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.StatRequest;

import java.util.Arrays;

public class StatFacet implements StatBuilder {

	private final StatRequest.Builder statRequestBuilder;

	public StatFacet(String numericField, String field) {
		statRequestBuilder = StatRequest.newBuilder().setNumericField(numericField).setFacetField(ZuliaQuery.Facet.newBuilder().setLabel(field).build());
	}

	public StatFacet(String numericField, String field, String... path) {
		this(numericField, field, Arrays.asList(path));
	}

	public StatFacet(String numericField, String field, Iterable<String> path) {
		statRequestBuilder = StatRequest.newBuilder().setNumericField(numericField)
				.setFacetField(ZuliaQuery.Facet.newBuilder().setLabel(field).addAllPath(path).build());
	}

	public StatFacet setTopN(int topN) {
		statRequestBuilder.setMaxFacets(topN);
		return this;
	}

	public StatFacet setTopNShard(int topNShard) {
		statRequestBuilder.setShardFacets(topNShard);
		return this;
	}

	@Override
	public StatRequest getStatRequest() {
		return statRequestBuilder.build();
	}
}
