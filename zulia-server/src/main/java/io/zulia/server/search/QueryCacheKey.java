package io.zulia.server.search;

import io.zulia.ZuliaFieldConstants;
import io.zulia.message.ZuliaQuery.CountRequest;
import io.zulia.message.ZuliaServiceOuterClass.QueryRequest;

import java.util.Comparator;
import java.util.List;

public class QueryCacheKey {

	private static final Comparator<CountRequest> FACET_ORDER = Comparator.comparing((CountRequest countRequest) -> countRequest.getFacetField().getLabel())
			.thenComparing(countRequest -> String.join(ZuliaFieldConstants.FACET_PATH_DELIMITER, countRequest.getFacetField().getPathList()));

	private final QueryRequest queryRequest;
	private final boolean pinned;

	public QueryCacheKey(QueryRequest queryRequest) {
		this.pinned = queryRequest.getPinToCache();

		// make sure it has the same signature as an unpinned search

		// remove the search label and concurrency from caching consideration as well

		// clear out all indexes from the request except for this index
		// this allows caching to happen at the index level, i.e. ->
		//  * the caching for identical queries searched again two indexes could be used for a combined query against two indexes
		//  * the two identical queries against different aliases pointed at the same index would be cache hits for each other

		QueryRequest.Builder keyBuilder = queryRequest.toBuilder().clearIndex().setPinToCache(false).setSearchLabel("").setConcurrency(1);

		// QueryCombiner restores the caller's facet group order, so count request order must not split the cache.
		// Warming searches stored by 5.6.0 or earlier hold their count requests in hash order.
		if (keyBuilder.getFacetRequest().getCountRequestCount() > 1) {
			List<CountRequest> sorted = keyBuilder.getFacetRequest().getCountRequestList().stream().sorted(FACET_ORDER).toList();
			keyBuilder.getFacetRequestBuilder().clearCountRequest().addAllCountRequest(sorted);
		}

		this.queryRequest = keyBuilder.build();
	}

	public boolean isPinned() {
		return pinned;
	}

	public int getSize() {
		return queryRequest.getSerializedSize();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((queryRequest == null) ? 0 : queryRequest.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		QueryCacheKey other = (QueryCacheKey) obj;
		if (queryRequest == null) {
			if (other.queryRequest != null) {
				return false;
			}
		}
		else if (!queryRequest.equals(other.queryRequest)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return queryRequest.toString();
	}
}
