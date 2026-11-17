package io.zulia.server.index.cache;

import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.writercache.TaxonomyWriterCache;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Bounded per dimension taxonomy writer cache. Returning true from put asks DirectoryTaxonomyWriter to reopen its taxonomy
 * reader, which is expensive, so it is only requested when an evicted category is newer than the last reopen and a later lookup
 * could otherwise miss it on disk and assign a second ordinal. See {@link DimensionCache} for the rule.
 */
public class ZuliaTaxonomyWriterCache implements TaxonomyWriterCache {

	private final int maxOrdinalsPerDimension;
	private final ConcurrentHashMap<FacetLabel, Integer> topLevelCache;
	private final ConcurrentHashMap<String, DimensionCache> dimensionCaches;

	public ZuliaTaxonomyWriterCache(int maxOrdinalsPerDimension) {
		this.maxOrdinalsPerDimension = maxOrdinalsPerDimension;
		this.topLevelCache = new ConcurrentHashMap<>();
		this.dimensionCaches = new ConcurrentHashMap<>();
	}

	private String getDimension(FacetLabel categoryPath) {
		return categoryPath.components[0];
	}

	@Override
	public void close() {
		clear();
	}

	@Override
	public int get(FacetLabel categoryPath) {
		if (categoryPath.length <= 1) {
			Integer ordinal = topLevelCache.get(categoryPath);
			return ordinal != null ? ordinal : -1;
		}
		DimensionCache dimensionCache = dimensionCaches.get(getDimension(categoryPath));
		return dimensionCache != null ? dimensionCache.get(categoryPath) : -1;
	}

	@Override
	public boolean put(FacetLabel categoryPath, int ordinal) {
		if (categoryPath.length <= 1) {
			topLevelCache.put(categoryPath, ordinal);
			return false;
		}
		return dimensionCaches.computeIfAbsent(getDimension(categoryPath), k -> new DimensionCache(maxOrdinalsPerDimension))
				.put(categoryPath, ordinal);
	}

	@Override
	public boolean isFull() {
		return true;
	}

	@Override
	public void clear() {
		topLevelCache.clear();
		dimensionCaches.values().forEach(DimensionCache::clear);
		dimensionCaches.clear();
	}

	@Override
	public int size() {
		long size = topLevelCache.size();
		for (DimensionCache dimensionCache : dimensionCaches.values()) {
			size += dimensionCache.size();
		}
		return (int) size;
	}
}
