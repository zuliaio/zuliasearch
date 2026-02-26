package io.zulia.util.pool;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

public class SemaphoreLimitedVirtualPool extends VirtualThreadPerTaskTaskExecutor {
	private final Semaphore pool;

	public SemaphoreLimitedVirtualPool(int threads) {
		pool = new Semaphore(threads);
	}

	public SemaphoreLimitedVirtualPool(int threads, String name) {
		super(name);
		pool = new Semaphore(threads);
	}

	@Override
	public <T> ListenableFuture<T> executeAsync(Callable<T> task) {
		try {
			pool.acquire();
			return super.executeAsync(() -> {
				try {
					return task.call();
				}
				finally {
					pool.release();
				}
			});
		}
		catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

	}

}
