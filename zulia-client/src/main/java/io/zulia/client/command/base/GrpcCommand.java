package io.zulia.client.command.base;

import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.Result;

public abstract class GrpcCommand<R extends Result> implements BaseCommand<R> {

	public abstract R execute(ZuliaConnection zuliaConnection);

	public R executeTimed(ZuliaConnection zuliaConnection) throws Exception {
		long start = System.currentTimeMillis();
		R r = execute(zuliaConnection);
		long end = System.currentTimeMillis();
		r.setCommandTimeMs(end - start);
		return r;
	}
}
