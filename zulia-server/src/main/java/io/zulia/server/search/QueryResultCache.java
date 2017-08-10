package io.zulia.server.search;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.zulia.message.ZuliaQuery.ShardQueryResponse;

public class QueryResultCache {
	private Cache<QueryCacheKey, ShardQueryResponse> queryResultCache;

	public QueryResultCache(int maxSize, int concurrency) {
		queryResultCache = CacheBuilder.newBuilder().concurrencyLevel(concurrency).maximumSize(maxSize).build();
	}

	public ShardQueryResponse getCachedShardQueryResponse(QueryCacheKey queryCacheKey) {
		return queryResultCache.getIfPresent(queryCacheKey);
	}

	public void storeInCache(QueryCacheKey queryCacheKey, ShardQueryResponse shardQueryResponse) {
		queryResultCache.put(queryCacheKey, shardQueryResponse);
	}

	public void clear() {
		queryResultCache.invalidateAll();
	}
}
