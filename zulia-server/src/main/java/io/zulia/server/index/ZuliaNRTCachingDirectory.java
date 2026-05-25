package io.zulia.server.index;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.NRTCachingDirectory;

import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;

/**
 * NRTCachingDirectory whose cache thresholds are read live from suppliers (backed by the index config) on every
 * caching decision, so changes made via updateIndexSettings take effect without reopening the IndexWriter.
 * <p>
 * The thresholds only gate whether new segments are cached. Lowering a threshold or disabling caching does not
 * force-evict already-cached bytes; Lucene only drains the RAM cache to the delegate on sync (commit), rename, or
 * close. So a shrink/disable takes full effect by the next commit.
 */
public class ZuliaNRTCachingDirectory extends NRTCachingDirectory {

	private final IntSupplier maxMergeSizeMB;
	private final IntSupplier maxCachedMB;
	private final BooleanSupplier cachingDisabled;

	public ZuliaNRTCachingDirectory(Directory delegate, IntSupplier maxMergeSizeMB, IntSupplier maxCachedMB, BooleanSupplier cachingDisabled) {
		// The super thresholds are only used by NRTCachingDirectory#toString; doCacheWrite is fully overridden below.
		super(delegate, maxMergeSizeMB.getAsInt(), maxCachedMB.getAsInt());
		this.maxMergeSizeMB = maxMergeSizeMB;
		this.maxCachedMB = maxCachedMB;
		this.cachingDisabled = cachingDisabled;
	}

	@Override
	protected boolean doCacheWrite(String name, IOContext context) {
		if (cachingDisabled.getAsBoolean()) {
			return false;
		}

		long bytes;
		if (context.mergeInfo() != null) {
			bytes = context.mergeInfo().estimatedMergeBytes();
		}
		else if (context.flushInfo() != null) {
			bytes = context.flushInfo().estimatedSegmentSize();
		}
		else {
			return false;
		}

		long maxMergeSizeBytes = (long) maxMergeSizeMB.getAsInt() * 1024 * 1024;
		long maxCachedBytes = (long) maxCachedMB.getAsInt() * 1024 * 1024;
		return (bytes <= maxMergeSizeBytes) && (bytes + ramBytesUsed()) <= maxCachedBytes;
	}

	@Override
	public String toString() {
		// Reflects the live supplier values, not the (ignored) super constructor args.
		return "ZuliaNRTCachingDirectory(" + in + "; disabled=" + cachingDisabled.getAsBoolean() + " maxMergeSizeMB=" + maxMergeSizeMB.getAsInt() + " maxCachedMB="
				+ maxCachedMB.getAsInt() + " ramUsedMB=" + (ramBytesUsed() / 1024. / 1024.) + ")";
	}
}
