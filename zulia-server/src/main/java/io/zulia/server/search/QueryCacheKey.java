package io.zulia.server.search;

import io.zulia.message.ZuliaServiceOuterClass.QueryRequest;

public class QueryCacheKey {

	private final QueryRequest queryRequest;
	private final boolean pinned;

	public QueryCacheKey(QueryRequest queryRequest) {
		this.pinned = queryRequest.getPinToCache();

		// make sure it has the same signature as an unpinned search

		// remove the search label from caching consideration as well

		// clear out all indexes from the request except for this index
		// this allows caching to happen at the index level, i.e. ->
		//  * the caching for identical queries searched again two indexes could be use for a combined query against two indexes
		//  * the two identical queries against different aliases pointed at the same index would be cache hits for each other

		this.queryRequest = queryRequest.toBuilder().clearIndex().setPinToCache(false).setSearchLabel("").build();
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
