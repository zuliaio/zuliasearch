package io.zulia.server.search;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.zulia.message.ZuliaQuery.ShardQueryResponse;

import java.util.concurrent.ConcurrentHashMap;

public class QueryResultCache {
	private Cache<QueryCacheKey, ShardQueryResponse> queryResultCache;
	private ConcurrentHashMap<QueryCacheKey, ShardQueryResponse> pinnedQueryResultCache;

	public QueryResultCache(int maxSize, int concurrency) {
		queryResultCache = CacheBuilder.newBuilder().concurrencyLevel(concurrency).maximumSize(maxSize).build();
		pinnedQueryResultCache = new ConcurrentHashMap<>();
	}

	public ShardQueryResponse getCachedShardQueryResponse(QueryCacheKey queryCacheKey) {
		ShardQueryResponse pinnedCacheHit = pinnedQueryResultCache.get(queryCacheKey);
		if (pinnedCacheHit == null) {
			return queryResultCache.getIfPresent(queryCacheKey);
		}
		return pinnedCacheHit;
	}

	public void storeInCache(QueryCacheKey queryCacheKey, ShardQueryResponse shardQueryResponse) {
		if (queryCacheKey.isPinned()) {
			pinnedQueryResultCache.put(queryCacheKey, shardQueryResponse);
		}
		else {
			queryResultCache.put(queryCacheKey, shardQueryResponse);
		}
	}

	public void clear() {
		queryResultCache.invalidateAll();
		pinnedQueryResultCache.clear();
	}
}
