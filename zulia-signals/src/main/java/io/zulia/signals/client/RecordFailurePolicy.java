package io.zulia.signals.client;

public enum RecordFailurePolicy {
	/** The default. */
	PROPAGATE,
	/** Log the failure, throttled, and return a result marked not stored. */
	LOG_AND_DROP
}
