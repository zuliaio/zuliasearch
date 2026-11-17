package io.zulia.server.index.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import org.apache.lucene.facet.taxonomy.FacetLabel;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One facet dimension's bounded taxonomy writer cache. The generation counts reader refreshes requested for this dimension. An entry
 * whose generation equals the current one was added after the last refresh, so the reader cannot know it yet.
 */
final class DimensionCache {

	private final Cache<FacetLabel, CachedOrdinal> cache;
	private final AtomicLong generation = new AtomicLong();
	private final AtomicBoolean unseenEvicted = new AtomicBoolean();

	DimensionCache(int maxOrdinals) {
		cache = CacheBuilder.newBuilder().maximumSize(maxOrdinals).<FacetLabel, CachedOrdinal>removalListener(notification -> {
			if (notification.getCause() == RemovalCause.SIZE && notification.getValue().generation() == generation.get()) {
				unseenEvicted.set(true);
			}
		}).build();
	}

	/** Returns the cached ordinal, or -1 when the category is not cached. */
	int get(FacetLabel categoryPath) {
		CachedOrdinal cached = cache.getIfPresent(categoryPath);
		return cached != null ? cached.ordinal() : -1;
	}

	/** Caches the ordinal and returns true when the taxonomy reader must be refreshed. */
	boolean put(FacetLabel categoryPath, int ordinal) {
		// Guava runs the removal listener inside this put when the insert evicts an entry
		cache.put(categoryPath, new CachedOrdinal(ordinal, generation.get()));
		// The cache is access ordered, so a lookup of any other entry demotes a just added category toward eviction. Evicting a
		// category the reader has not seen yet is the only way a later lookup can assign it a second ordinal, so that eviction
		// requests a refresh. Evicting a category from an earlier generation is free, the reader already holds it.
		if (unseenEvicted.getAndSet(false)) {
			generation.incrementAndGet();
			return true;
		}
		return false;
	}

	long size() {
		return cache.size();
	}

	void clear() {
		cache.invalidateAll();
	}
}
