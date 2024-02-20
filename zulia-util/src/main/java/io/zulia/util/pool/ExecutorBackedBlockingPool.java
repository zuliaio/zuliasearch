package io.zulia.util.pool;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExecutorBackedBlockingPool implements TaskExecutor {
	private final ListeningExecutorService pool;

	public ExecutorBackedBlockingPool(int threads, int maxQueued, ThreadFactory threadFactory) {
		pool = MoreExecutors.listeningDecorator(
				new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS, new WaitingArrayBlockingQueue<>(maxQueued), threadFactory));

	}

	@Override
	public <T> ListenableFuture<T> executeAsync(Callable<T> task) {
		return pool.submit(task);
	}

	@Override
	public void close() {
		pool.close();
	}
}
