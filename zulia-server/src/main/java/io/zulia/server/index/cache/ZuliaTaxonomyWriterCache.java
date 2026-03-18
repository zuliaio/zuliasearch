package io.zulia.server.index.cache;

import com.koloboke.collect.map.ObjIntMap;
import com.koloboke.collect.map.hash.HashObjIntMaps;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.writercache.TaxonomyWriterCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ZuliaTaxonomyWriterCache implements TaxonomyWriterCache {

	private static final int DEFAULT_CONCURRENCY = 16;

	private final int concurrency;
	private final ObjIntMap<FacetLabel>[] segments;
	private final Lock[] locks;

	public ZuliaTaxonomyWriterCache(int expectedSize) {
		this(expectedSize, DEFAULT_CONCURRENCY);
	}

	@SuppressWarnings("unchecked")
	public ZuliaTaxonomyWriterCache(int expectedSize, int concurrency) {
		this.concurrency = concurrency;
		this.segments = new ObjIntMap[concurrency];
		this.locks = new Lock[concurrency];
		int segmentSize = Math.max(1, expectedSize / concurrency);
		for (int i = 0; i < concurrency; i++) {
			segments[i] = HashObjIntMaps.getDefaultFactory().withDefaultValue(-1).newMutableMap(segmentSize);
			locks[i] = new ReentrantLock();
		}
	}

	private int segmentIndex(FacetLabel categoryPath) {
		return Math.floorMod(categoryPath.hashCode(), concurrency);
	}

	@Override
	public void close() {
		for (int i = 0; i < concurrency; i++) {
			locks[i].lock();
			try {
				segments[i] = null;
			}
			finally {
				locks[i].unlock();
			}
		}
	}

	@Override
	public int get(FacetLabel categoryPath) {
		int idx = segmentIndex(categoryPath);
		locks[idx].lock();
		try {
			return segments[idx].getInt(categoryPath);
		}
		finally {
			locks[idx].unlock();
		}
	}

	@Override
	public boolean put(FacetLabel categoryPath, int ordinal) {
		int idx = segmentIndex(categoryPath);
		locks[idx].lock();
		try {
			segments[idx].put(categoryPath, ordinal);
		}
		finally {
			locks[idx].unlock();
		}
		return false;
	}

	@Override
	public boolean isFull() {
		return false;
	}

	@Override
	public void clear() {
		for (int i = 0; i < concurrency; i++) {
			locks[i].lock();
			try {
				segments[i].clear();
			}
			finally {
				locks[i].unlock();
			}
		}
	}

	@Override
	public int size() {
		int size = 0;
		for (int i = 0; i < concurrency; i++) {
			locks[i].lock();
			try {
				size += segments[i].size();
			}
			finally {
				locks[i].unlock();
			}
		}
		return size;
	}
}
