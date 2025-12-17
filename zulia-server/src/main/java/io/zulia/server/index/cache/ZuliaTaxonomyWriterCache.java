package io.zulia.server.index.cache;

import com.koloboke.collect.map.ObjIntMap;
import com.koloboke.collect.map.hash.HashObjIntMaps;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.writercache.TaxonomyWriterCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ZuliaTaxonomyWriterCache implements TaxonomyWriterCache {

	private ObjIntMap<FacetLabel> cache;
	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private final Lock r = lock.readLock();
	private final Lock w = lock.writeLock();

	public ZuliaTaxonomyWriterCache() {
		cache = HashObjIntMaps.getDefaultFactory().withDefaultValue(-1).newMutableMap();
	}

	@Override
	public void close() {
		clear();
		cache = null;
	}

	@Override
	public int get(FacetLabel categoryPath) {
		r.lock();
		try {
			return cache.getInt(categoryPath);
		}
		finally {
			r.unlock();
		}
	}

	@Override
	public boolean put(FacetLabel categoryPath, int ordinal) {
		w.lock();
		try {
			cache.put(categoryPath, ordinal);
		}
		finally {
			w.unlock();
		}
		return false;
	}

	@Override
	public boolean isFull() {
		return false;
	}

	@Override
	public void clear() {
		cache.clear();
	}

	@Override
	public int size() {
		return cache.size();
	}
}
