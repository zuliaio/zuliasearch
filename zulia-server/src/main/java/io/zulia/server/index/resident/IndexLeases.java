package io.zulia.server.index.resident;

import io.zulia.server.exceptions.IndexDoesNotExistException;
import io.zulia.server.index.ZuliaIndex;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Leases for the set of indexes an operation touches, keyed in acquisition order by the name the
 * caller chose (resolved index name or original request name). Closing releases every lease.
 */
public final class IndexLeases implements AutoCloseable {

	private final Map<String, IndexLease> leases;
	private final Map<String, ZuliaIndex> indexes;

	public IndexLeases(LinkedHashMap<String, IndexLease> leases) {
		this.leases = leases;
		LinkedHashMap<String, ZuliaIndex> indexByName = new LinkedHashMap<>();
		leases.forEach((name, lease) -> indexByName.put(name, lease.getIndex()));
		this.indexes = Collections.unmodifiableMap(indexByName);
	}

	/**
	 * Acquires a lease per distinct name, releasing any already acquired if one fails.
	 */
	public static IndexLeases acquire(Collection<String> indexNames, LeaseFunction leaser) throws Exception {
		LinkedHashMap<String, IndexLease> leases = new LinkedHashMap<>();
		try {
			for (String indexName : indexNames) {
				if (!leases.containsKey(indexName)) {
					IndexLease lease = leaser.lease(indexName);
					if (lease == null) {
						throw new IndexDoesNotExistException(indexName);
					}
					leases.put(indexName, lease);
				}
			}
			return new IndexLeases(leases);
		}
		catch (Exception e) {
			leases.values().forEach(IndexLease::close);
			throw e;
		}
	}

	public Map<String, ZuliaIndex> getIndexes() {
		return indexes;
	}

	@Override
	public void close() {
		leases.values().forEach(IndexLease::close);
	}
}
