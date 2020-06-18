package io.zulia.client.command.base;

import io.zulia.client.pool.ZuliaPool;
import io.zulia.client.result.Result;

import java.util.concurrent.Callable;

public class CallableCommand<R extends Result> implements Callable<R> {

	private BaseCommand<R> command;
	private ZuliaPool zuliaPool;

	public CallableCommand(ZuliaPool zuliaPool, BaseCommand<R> command) {
		this.zuliaPool = zuliaPool;
		this.command = command;
	}

	@Override
	public R call() throws Exception {
		return zuliaPool.execute(command);
	}
}

