package io.zulia.util.pool;

import java.util.concurrent.atomic.AtomicInteger;

public class WorkPool {

	private final static AtomicInteger nativePoolNumber = new AtomicInteger(1);

	/**
	 * creates a TaskExecutor using virtual threads that will not allow more than maxThreads to run at the same time
	 * and blocks when more than maxThreads*10 tasks are waiting
	 *
	 * @param maxThreads - tasks to run at the same time
	 */
	public static TaskExecutor nativePool(int maxThreads) {
		return nativePool(maxThreads, maxThreads * 10);
	}

	/**
	 * creates a TaskExecutor using native threads that will not allow more than maxThreads to run at the same time
	 * and blocks when more than maxThreads*10 tasks are waiting. If using more than 128 threads, consider using virtual threads.
	 *
	 * @param maxThreads - tasks to run at the same time
	 * @param poolName   - name prefix of the pool threads for logging / debugging
	 */
	public static TaskExecutor nativePool(int maxThreads, String poolName) {
		return new ExecutorBackedBlockingPool(maxThreads, maxThreads * 10, new NamedThreadFactory(poolName));
	}

	/**
	 * creates a TaskExecutor using native threads that will not allow more than maxThreads to run at the same time
	 * and blocks when more than maxQueued tasks are waiting. If using more than 128 threads, consider using virtual threads.
	 *
	 * @param maxThreads - tasks to run at the same time
	 * @param maxQueued  - max tasks to be queued
	 */
	public static TaskExecutor nativePool(int maxThreads, int maxQueued) {
		return new ExecutorBackedBlockingPool(maxThreads, maxQueued, new NamedThreadFactory("workPool-" + nativePoolNumber.getAndIncrement()));
	}

	/**
	 * creates a TaskExecutor using native threads that will not allow more than maxThreads to run at the same time
	 * and blocks when more than maxQueued tasks are waiting. If using more than 128 threads, consider using virtual threads.
	 *
	 * @param maxThreads - tasks to run at the same time
	 * @param maxQueued  - max tasks to be queued
	 * @param poolName   - name prefix of the pool threads for logging / debugging
	 */
	public static TaskExecutor nativePool(int maxThreads, int maxQueued, String poolName) {
		return new ExecutorBackedBlockingPool(maxThreads, maxQueued, new NamedThreadFactory(poolName));
	}

	/**
	 * creates a TaskExecutor using virtual threads that will not allow more than maxThreads to run at the same time
	 * and blocks when more than maxThreads*10 tasks are waiting
	 * prefer {@link #virtualBounded(int)}
	 * @param maxThreads - tasks to run at the same time
	 */
	public static TaskExecutor virtualPool(int maxThreads) {
		return new ExecutorBackedBlockingPool(maxThreads, maxThreads * 10, Thread.ofVirtual().factory());
	}

	/**
	 * creates a TaskExecutor using virtual threads that will not allow more than maxThreads to run at the same time
	 * and blocks when more than maxQueued tasks are waiting
	 * prefer {@link #virtualBounded(int)}
	 * @param maxThreads - tasks to run at the same time
	 * @param maxQueued  - max tasks to be queued
	 */
	public static TaskExecutor virtualPool(int maxThreads, int maxQueued) {
		return new ExecutorBackedBlockingPool(maxThreads, maxQueued, Thread.ofVirtual().factory());
	}

	/**
	 * Creates a TaskExecutor that starts a virtual thread per task without any limits on the number of virtual threads created
	 */
	public static TaskExecutor virtualUnbounded() {
		return new VirtualThreadPerTaskTaskExecutor();
	}

	/**
	 * Creates a TaskExecutor that starts a virtual thread per task but limits concurrent threads using a semaphore
	 * As per <a href="https://inside.java/2024/02/04/sip094/">Managing Throughput with Virtual Threads - Sip of Java</a>
	 */
	public static TaskExecutor virtualBounded(int maxThreads) {
		return new SemaphoreLimitedVirtualPool(maxThreads);
	}

}
