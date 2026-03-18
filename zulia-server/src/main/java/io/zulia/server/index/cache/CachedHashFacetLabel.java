package io.zulia.server.index.cache;

import org.apache.lucene.facet.taxonomy.FacetLabel;

/**
 * A {@link FacetLabel} subclass that caches the hash code for efficient use as a hash map key.
 */
public class CachedHashFacetLabel extends FacetLabel {

	private int cachedHashCode;

	public CachedHashFacetLabel(String... components) {
		super(components);
	}

	public CachedHashFacetLabel(String dim, String[] path) {
		super(dim, path);
	}

	@Override
	public int hashCode() {
		int h = cachedHashCode;
		if (h == 0 && length > 0) {
			h = super.hashCode();
			cachedHashCode = h;
		}
		return h;
	}

	@Override
	public FacetLabel subpath(int length) {
		if (length >= this.length || length < 0) {
			return this;
		}
		String[] sub = new String[length];
		System.arraycopy(components, 0, sub, 0, length);
		return new CachedHashFacetLabel(sub);
	}
}
