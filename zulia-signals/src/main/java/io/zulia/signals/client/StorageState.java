package io.zulia.signals.client;

import java.time.Instant;

/** Shared by the onFailure copies of one client. */
final class StorageState {

	private volatile String readyIndex;
	private volatile Instant lastAttempt;

	/** The physical index storage was last created for, null until the first success. Its name carries the month when partitioned. */
	String getReadyIndex() {
		return readyIndex;
	}

	void setReadyIndex(String readyIndex) {
		this.readyIndex = readyIndex;
	}

	Instant getLastAttempt() {
		return lastAttempt;
	}

	void setLastAttempt(Instant lastAttempt) {
		this.lastAttempt = lastAttempt;
	}
}
