package io.zulia.client.command.base;

import io.zulia.client.ZuliaRESTClient;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.Result;

public abstract class RESTCommand<R extends Result> extends Command<R> {

	public abstract R execute(ZuliaRESTClient zuliaRESTClient) throws Exception;

	@Override
	public R execute(ZuliaConnection zuliaConnection) throws Exception {
		return execute(zuliaConnection.getRestClient());
	}

}
