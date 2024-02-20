package io.zulia.util.pool;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

public class VirtualThreadPerTaskTaskExecutor implements TaskExecutor {

	private final ListeningExecutorService executorService;

	public VirtualThreadPerTaskTaskExecutor() {
		executorService = MoreExecutors.listeningDecorator(Executors.newVirtualThreadPerTaskExecutor());
	}

	@Override
	public <T> ListenableFuture<T> executeAsync(Callable<T> task) {
		return executorService.submit(task);
	}

	@Override
	public void close() {
		executorService.close();
	}
}
