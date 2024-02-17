package io.zulia.util.pool;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Very similar to NamedThreadFactory in lucene but keeps from depending on lucene here
 */
public class NamedThreadFactory implements ThreadFactory {

	private final AtomicInteger threadCounter = new AtomicInteger(1);
	private final ThreadGroup threadGroup;
	private final String prefix;

	public NamedThreadFactory(String prefix) {
		threadGroup = Thread.currentThread().getThreadGroup();
		this.prefix = prefix;
	}

	@Override
	public Thread newThread(Runnable r) {
		Thread t = new Thread(threadGroup, r, prefix + "-" + threadCounter.getAndIncrement(), 0);

		t.setDaemon(true);

		if (t.getPriority() != Thread.NORM_PRIORITY) {
			t.setPriority(Thread.NORM_PRIORITY);
		}
		return t;
	}
}
