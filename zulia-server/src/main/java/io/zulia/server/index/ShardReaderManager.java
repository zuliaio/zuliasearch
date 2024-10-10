package io.zulia.server.index;

import org.apache.lucene.search.ReferenceManager;

import java.io.IOException;

public class ShardReaderManager extends ReferenceManager<ShardReader> {

	private long latestShardTime;

	public ShardReaderManager(ShardReader initial) {
		this.current = initial;
		this.latestShardTime = initial.getCreationTime();
	}

	@Override
	protected void decRef(ShardReader reference) throws IOException {
		reference.decRef();
	}

	@Override
	protected ShardReader refreshIfNeeded(ShardReader referenceToRefresh) throws IOException {
		// Evaluate last build time for outside decision making
		ShardReader next = referenceToRefresh.refreshIfNeeded();
		if (next != null) {
			latestShardTime = next.getCreationTime();
		}
		return next;
	}

	@Override
	protected boolean tryIncRef(ShardReader reference) throws IOException {
		return reference.tryIncRef();
	}

	@Override
	protected int getRefCount(ShardReader reference) {
		return reference.getRefCount();
	}

	public long getLatestShardTime() {
		return latestShardTime;
	}
}
