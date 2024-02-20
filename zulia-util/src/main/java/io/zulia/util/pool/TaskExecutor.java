package io.zulia.util.pool;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public interface TaskExecutor extends AutoCloseable {

	<T> ListenableFuture<T> executeAsync(Callable<T> task);

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

	void close();
}
