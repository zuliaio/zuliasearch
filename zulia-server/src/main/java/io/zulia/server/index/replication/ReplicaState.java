package io.zulia.server.index.replication;

class ReplicaState {

	private static final int FAILURE_THRESHOLD = 3;
	private static final long BASE_BACKOFF_MS = 10_000;
	private static final long MAX_BACKOFF_MS = 5 * 60_000;

	static final Snapshot EMPTY_SNAPSHOT = new Snapshot(null, null, 0, false, 0);

	private Long generation;
	private Long lastSuccessMs;
	private int consecutiveFailures;
	private long backoffUntilMs;

	synchronized boolean isCircuitOpen() {
		return backoffUntilMs > System.currentTimeMillis();
	}

	synchronized Long getGeneration() {
		return generation;
	}

	synchronized void recordSuccess(long replicatedGeneration) {
		this.generation = replicatedGeneration;
		this.lastSuccessMs = System.currentTimeMillis();
		this.consecutiveFailures = 0;
		this.backoffUntilMs = 0;
	}

	synchronized FailureOutcome recordFailure() {
		consecutiveFailures++;
		long openedBackoffMs = 0;
		if (consecutiveFailures >= FAILURE_THRESHOLD) {
			int exponent = consecutiveFailures - FAILURE_THRESHOLD;
			openedBackoffMs = Math.min(MAX_BACKOFF_MS, BASE_BACKOFF_MS << Math.min(exponent, 10));
			backoffUntilMs = System.currentTimeMillis() + openedBackoffMs;
		}
		return new FailureOutcome(consecutiveFailures, openedBackoffMs);
	}

	synchronized Snapshot snapshot(long now) {
		return new Snapshot(generation, lastSuccessMs, consecutiveFailures, backoffUntilMs > now, backoffUntilMs);
	}

	// openedBackoffMs is 0 unless this failure (re-)opened the breaker, in which case it is the new backoff.
	record FailureOutcome(int consecutiveFailures, long openedBackoffMs) {
	}

	record Snapshot(Long generation, Long lastSuccessMs, int consecutiveFailures, boolean circuitOpen, long backoffUntilMs) {
	}
}
