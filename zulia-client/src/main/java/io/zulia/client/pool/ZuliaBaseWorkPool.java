package io.zulia.client.pool;

import com.google.common.util.concurrent.ListenableFuture;
import io.zulia.client.command.base.CallableCommand;
import io.zulia.client.command.base.Command;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.result.Result;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.zulia.message.ZuliaBase.Node;

public class ZuliaBaseWorkPool extends WorkPool {

	private ZuliaPool zuliaPool;

	private static AtomicInteger counter = new AtomicInteger(0);

	public ZuliaBaseWorkPool(ZuliaPoolConfig zuliaPoolConfig) throws Exception {
		this(new ZuliaPool(zuliaPoolConfig), zuliaPoolConfig.getPoolName() != null ? zuliaPoolConfig.getPoolName() : "zuliaPool-" + counter.getAndIncrement());
	}

	public ZuliaBaseWorkPool(ZuliaPool zuliaPool) {
		this(zuliaPool, "zuliaPool-" + counter.getAndIncrement());
	}

	public ZuliaBaseWorkPool(ZuliaPool zuliaPool, String poolName) {
		super(zuliaPool.getMaxConnections(), zuliaPool.getMaxConnections() * 10, poolName);
		this.zuliaPool = zuliaPool;
	}

	public <R extends Result> ListenableFuture<R> executeAsync(Command<R> command) {
		CallableCommand<R> callableCommand = new CallableCommand<>(zuliaPool, command);
		return executeAsync(callableCommand);
	}

	public <R extends Result> R execute(Command<R> command) throws Exception {
		CallableCommand<R> callableCommand = new CallableCommand<>(zuliaPool, command);
		return execute(callableCommand);
	}

	public void updateNodes(List<Node> nodes) throws Exception {
		zuliaPool.updateNodes(nodes);
	}

	@Override
	public void shutdown() throws Exception {
		super.shutdown();
		zuliaPool.close();
	}
}
