package io.zulia.server.index.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.writercache.TaxonomyWriterCache;

import java.util.concurrent.ConcurrentHashMap;

public class ZuliaTaxonomyWriterCache implements TaxonomyWriterCache {

	private static final int MAX_ORDINALS_PER_DIMENSION = 4096;

	private final ConcurrentHashMap<FacetLabel, Integer> topLevelCache;
	private final ConcurrentHashMap<String, Cache<FacetLabel, Integer>> dimensionCaches;

	public ZuliaTaxonomyWriterCache() {
		this.topLevelCache = new ConcurrentHashMap<>();
		this.dimensionCaches = new ConcurrentHashMap<>();
	}

	private Cache<FacetLabel, Integer> getOrCreateDimensionCache(String dimension) {
		return dimensionCaches.computeIfAbsent(dimension,
				k -> CacheBuilder.newBuilder().maximumSize(MAX_ORDINALS_PER_DIMENSION).build());
	}

	private String getDimension(FacetLabel categoryPath) {
		return categoryPath.components[0];
	}

	@Override
	public void close() {
		topLevelCache.clear();
		dimensionCaches.values().forEach(Cache::invalidateAll);
		dimensionCaches.clear();
	}

	@Override
	public int get(FacetLabel categoryPath) {
		if (categoryPath.length <= 1) {
			Integer ordinal = topLevelCache.get(categoryPath);
			return ordinal != null ? ordinal : -1;
		}
		String dimension = getDimension(categoryPath);
		Cache<FacetLabel, Integer> cache = dimensionCaches.get(dimension);
		if (cache == null) {
			return -1;
		}
		Integer ordinal = cache.getIfPresent(categoryPath);
		return ordinal != null ? ordinal : -1;
	}

	@Override
	public boolean put(FacetLabel categoryPath, int ordinal) {
		if (categoryPath.length <= 1) {
			topLevelCache.put(categoryPath, ordinal);
			return false;
		}
		String dimension = getDimension(categoryPath);
		Cache<FacetLabel, Integer> cache = getOrCreateDimensionCache(dimension);
		cache.put(categoryPath, ordinal);
		return false;
	}

	@Override
	public boolean isFull() {
		return true;
	}

	@Override
	public void clear() {
		topLevelCache.clear();
		dimensionCaches.values().forEach(Cache::invalidateAll);
		dimensionCaches.clear();
	}

	@Override
	public int size() {
		long size = topLevelCache.size();
		for (Cache<FacetLabel, Integer> cache : dimensionCaches.values()) {
			size += cache.size();
		}
		return (int) size;
	}
}
