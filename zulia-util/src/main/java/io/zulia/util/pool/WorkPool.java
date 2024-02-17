package io.zulia.util.pool;

import java.util.concurrent.atomic.AtomicInteger;

public class WorkPool {

	private final static AtomicInteger nativePoolNumber = new AtomicInteger(1);

	public static TaskExecutor nativePool(int maxThreads) {
		return nativePool(maxThreads, maxThreads * 10);
	}

	public static TaskExecutor nativePool(int maxThreads, String poolName) {
		return new ExecutorBackedBlockingPool(maxThreads, maxThreads * 10, new NamedThreadFactory(poolName));
	}

	public static TaskExecutor nativePool(int maxThreads, int maxQueued) {
		return new ExecutorBackedBlockingPool(maxThreads, maxQueued, new NamedThreadFactory("workPool-" + nativePoolNumber.getAndIncrement()));
	}

	public static TaskExecutor nativePool(int maxThreads, int maxQueued, String poolName) {
		return new ExecutorBackedBlockingPool(maxThreads, maxQueued, new NamedThreadFactory(poolName));
	}

	public static TaskExecutor virtualPool(int maxThreads) {
		return new ExecutorBackedBlockingPool(maxThreads, maxThreads * 10, Thread.ofVirtual().factory());
	}

	public static TaskExecutor virtualPool(int maxThreads, int maxQueued) {
		return new ExecutorBackedBlockingPool(maxThreads, maxQueued, Thread.ofVirtual().factory());
	}

}
