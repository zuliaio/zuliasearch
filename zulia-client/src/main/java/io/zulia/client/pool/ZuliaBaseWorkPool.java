package io.zulia.client.pool;

import com.google.common.util.concurrent.ListenableFuture;
import io.zulia.client.command.base.BaseCommand;
import io.zulia.client.command.base.CallableCommand;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.result.Result;
import io.zulia.util.pool.TaskExecutor;
import io.zulia.util.pool.WorkPool;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.zulia.message.ZuliaBase.Node;

public class ZuliaBaseWorkPool implements AutoCloseable {

	private boolean closed;
	private final TaskExecutor taskExecutor;
	private ZuliaPool zuliaPool;

	private static AtomicInteger counter = new AtomicInteger(0);

	public ZuliaBaseWorkPool(ZuliaPoolConfig zuliaPoolConfig) {
		this(new ZuliaPool(zuliaPoolConfig), zuliaPoolConfig.getPoolName() != null ? zuliaPoolConfig.getPoolName() : "zuliaPool-" + counter.getAndIncrement());
	}

	public ZuliaBaseWorkPool(ZuliaPool zuliaPool) {
		this(zuliaPool, "zuliaPool-" + counter.getAndIncrement());
	}

	public ZuliaBaseWorkPool(ZuliaPool zuliaPool, String poolName) {
		taskExecutor = WorkPool.virtualBounded(zuliaPool.getMaxConnections(), poolName);
		this.zuliaPool = zuliaPool;
	}

	public <R extends Result> ListenableFuture<R> executeAsync(BaseCommand<R> command) {
		CallableCommand<R> callableCommand = new CallableCommand<>(zuliaPool, command);
		return taskExecutor.executeAsync(callableCommand);
	}

	public <R extends Result> R execute(BaseCommand<R> command) throws Exception {
		CallableCommand<R> callableCommand = new CallableCommand<>(zuliaPool, command);
		return taskExecutor.execute(callableCommand);
	}

	public void updateNodesAndRouting() throws Exception {
		zuliaPool.updateNodesAndRouting();
	}

	public void updateNodes(List<Node> nodes) {
		zuliaPool.updateNodes(nodes);
	}

	@Deprecated
	public void shutdown() {
		close();
	}

	public void close() {
		synchronized (this) {
			if (!closed) {
				taskExecutor.close();
				zuliaPool.close();
				closed = true;
			}
		}
	}
}
