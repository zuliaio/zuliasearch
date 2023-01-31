package io.zulia.client.command.builder;

import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.StatRequest;

import java.util.Arrays;
import java.util.List;

public class StatFacet implements StatBuilder {

	private final StatRequest.Builder statRequestBuilder;

	public StatFacet(String numericField, String facetField) {
		statRequestBuilder = StatRequest.newBuilder().setNumericField(numericField).setFacetField(ZuliaQuery.Facet.newBuilder().setLabel(facetField).build());
	}

	public StatFacet(String numericField, String facetField, String... path) {
		this(numericField, facetField, Arrays.asList(path));
	}

	public StatFacet(String numericField, String facetField, Iterable<String> path) {
		statRequestBuilder = StatRequest.newBuilder().setNumericField(numericField)
				.setFacetField(ZuliaQuery.Facet.newBuilder().setLabel(facetField).addAllPath(path).build());
	}

	public StatFacet setPercentilePrecision(Double precision) {
		if (precision > 0.0) {
			statRequestBuilder.setPrecision(precision);
		}
		else {
			throw new IllegalArgumentException("Percentile precision must be > 0.0");
		}
		return this;
	}

	public StatFacet setPercentiles(List<Double> percentilePoints) {
		// Percentiles are a fraction
		if (percentilePoints.stream().allMatch(d -> (0.0 <= d && d <= 1.0))) {
			statRequestBuilder.addAllPercentiles(percentilePoints);
		}
		else {
			throw new IllegalArgumentException("Percentiles must be in the range [0.0, 1.0]");
		}
		return this;
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
