package io.zulia.server.search.aggregation;

public record AggregationSettings(int hitsPerConcurrentRequest, int maxFacetsCachedPerDimension) {

	public static final int DEFAULT_HITS_PER_CONCURRENT_REQUEST = 10000;
	public static final int DEFAULT_MAX_FACETS_CACHED_PER_DIMENSION = 4096;

	public AggregationSettings {
		if (hitsPerConcurrentRequest <= 0) {
			hitsPerConcurrentRequest = DEFAULT_HITS_PER_CONCURRENT_REQUEST;
		}
		if (maxFacetsCachedPerDimension <= 0) {
			maxFacetsCachedPerDimension = DEFAULT_MAX_FACETS_CACHED_PER_DIMENSION;
		}
	}

}
