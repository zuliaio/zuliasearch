package io.zulia.server.index.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.writercache.TaxonomyWriterCache;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ZuliaTaxonomyWriterCache implements TaxonomyWriterCache {

	private static final int DEFAULT_MAX_ORDINALS_PER_DIMENSION = 4096;

	private final int maxOrdinalsPerDimension;
	private final int evictionsBetweenRefreshes;
	private final ConcurrentHashMap<FacetLabel, Integer> topLevelCache;
	private final ConcurrentHashMap<String, Cache<FacetLabel, Integer>> dimensionCaches;
	private final Set<String> evictedDimensions;
	private final ConcurrentHashMap<String, AtomicLong> evictionCounters;

	public ZuliaTaxonomyWriterCache(int maxOrdinalsPerDimension) {
		this.maxOrdinalsPerDimension = maxOrdinalsPerDimension > 0 ? maxOrdinalsPerDimension : DEFAULT_MAX_ORDINALS_PER_DIMENSION;
		this.evictionsBetweenRefreshes = Math.max(1, this.maxOrdinalsPerDimension / 4);
		this.topLevelCache = new ConcurrentHashMap<>();
		this.dimensionCaches = new ConcurrentHashMap<>();
		this.evictedDimensions = Collections.newSetFromMap(new ConcurrentHashMap<>());
		this.evictionCounters = new ConcurrentHashMap<>();
	}

	private Cache<FacetLabel, Integer> getOrCreateDimensionCache(String dimension) {
		return dimensionCaches.computeIfAbsent(dimension, k -> CacheBuilder.newBuilder().maximumSize(maxOrdinalsPerDimension)
				.removalListener(notification -> {
					if (notification.getCause() == RemovalCause.SIZE) {
						evictedDimensions.add(dimension);
					}
				}).build());
	}

	private String getDimension(FacetLabel categoryPath) {
		return categoryPath.components[0];
	}

	@Override
	public void close() {
		topLevelCache.clear();
		dimensionCaches.values().forEach(Cache::invalidateAll);
		dimensionCaches.clear();
		evictedDimensions.clear();
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
		// Guava's removal listener fires synchronously during put() when an eviction occurs,
		// adding the dimension to evictedDimensions before this line completes
		cache.put(categoryPath, ordinal);
		// Returning true signals DirectoryTaxonomyWriter.addToCache() to refresh its reader so that
		// subsequent disk lookups for evicted categories can find their existing ordinals and avoid
		// assigning duplicates. Refreshing on every eviction is extremely expensive under high-
		// cardinality facets (each refresh reopens the NRT taxonomy reader). Instead, we batch
		// refreshes: accumulate evictions and only trigger a refresh every evictionsBetweenRefreshes.
		// This is safe because with an LRU cache of size N, a category cannot be evicted until N
		// newer entries displace it, so the reader has at least N/evictionsBetweenRefreshes refreshes
		// (at least 4 with the default ratio) before a re-encountered category could be missed by
		// findCategory().
		if (evictedDimensions.remove(dimension)) {
			AtomicLong counter = evictionCounters.computeIfAbsent(dimension, k -> new AtomicLong());
			return counter.incrementAndGet() % evictionsBetweenRefreshes == 0;
		}
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
		evictedDimensions.clear();
		evictionCounters.clear();
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
