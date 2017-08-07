package io.zulia.client.command.base;

import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.Result;

public abstract class Command<R extends Result> {

	public abstract R execute(ZuliaConnection zuliaConnection) throws Exception;

	public R executeTimed(ZuliaConnection zuliaConnection) throws Exception {
		long start = System.currentTimeMillis();
		R r = execute(zuliaConnection);
		long end = System.currentTimeMillis();
		r.setCommandTimeMs(end - start);
		return r;
	}
}
