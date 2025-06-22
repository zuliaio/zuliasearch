package io.zulia.util.pool;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

public interface TaskExecutor extends AutoCloseable, Executor {

	/**
	 * Executes callable in the background and returns a ListenableFuture
	 * @param task - task to execute
	 */
	<T> ListenableFuture<T> executeAsync(Callable<T> task);

	/**
	 * Executes callable in the background and waits for response
	 * @param task - task to execute
	 */
	default <T> T execute(Callable<T> task) throws Exception {

		try {
			return executeAsync(task).get();
		}
		catch (ExecutionException e) {
			Throwable cause = e.getCause();
			if (cause != null) {
				if (cause instanceof Exception ex) {
					throw ex;
				}
			}
			throw e;
		}

	}

	/**
	 * Executes runnable in the background
	 * @param runnable - runnable to run
	 */
	default void execute(Runnable runnable) {
		executeAsync(() -> {
			runnable.run();
			return null;
		});
	}

	void close();
}
