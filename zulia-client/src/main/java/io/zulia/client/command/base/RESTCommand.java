package io.zulia.client.command.base;

import io.zulia.client.ZuliaRESTClient;
import io.zulia.client.result.Result;

public abstract class RESTCommand<R extends Result> implements BaseCommand<R> {

	public abstract R execute(ZuliaRESTClient zuliaRESTClient) throws Exception;

}
