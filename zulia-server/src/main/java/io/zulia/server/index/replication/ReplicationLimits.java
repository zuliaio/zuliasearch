package io.zulia.server.index.replication;

public final class ReplicationLimits {

	public static final int CHUNK_SIZE = 1024 * 1024;
	// Receiver cap; kept above CHUNK_SIZE so a sender bump does not require a coordinated receiver release.
	public static final int MAX_CHUNK_BYTES = 8 * 1024 * 1024;

	private ReplicationLimits() {
	}
}
