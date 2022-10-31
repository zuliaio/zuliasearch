package io.zulia.server.search;

import io.zulia.message.ZuliaServiceOuterClass.QueryRequest;

public class QueryCacheKey {

	private QueryRequest queryRequest;
	private boolean pinned;

	public QueryCacheKey(QueryRequest queryRequest) {
		this.pinned = queryRequest.getPinToCache();
		this.queryRequest = queryRequest.toBuilder().setPinToCache(false).setSearchLabel("")
				.build(); // make sure it has the same signature as an unpinned search
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

}
