package io.zulia.server.index.resident;

import io.zulia.server.index.ZuliaIndex;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A hold on a resident index for the duration of an operation, released on close. While any lease is
 * held, the index must not be unloaded, so bounded residency can defer eviction until the
 * lease count reaches zero. Close is idempotent.
 */
public final class IndexLease implements AutoCloseable {

	private final IndexHandle handle;
	private final AtomicBoolean closed = new AtomicBoolean();

	IndexLease(IndexHandle handle) {
		this.handle = handle;
	}

	public ZuliaIndex getIndex() {
		return handle.getIndex();
	}

	@Override
	public void close() {
		if (closed.compareAndSet(false, true)) {
			handle.release();
		}
	}
}
