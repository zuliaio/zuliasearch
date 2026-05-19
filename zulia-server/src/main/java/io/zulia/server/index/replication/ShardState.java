package io.zulia.server.index.replication;

import java.util.concurrent.locks.ReentrantLock;

class ShardState {

	private final ReentrantLock lock = new ReentrantLock();
	private volatile Long lastAttemptedGeneration;

	ReentrantLock getLock() {
		return lock;
	}

	Long getLastAttemptedGeneration() {
		return lastAttemptedGeneration;
	}

	void setLastAttemptedGeneration(Long lastAttemptedGeneration) {
		this.lastAttemptedGeneration = lastAttemptedGeneration;
	}
}
