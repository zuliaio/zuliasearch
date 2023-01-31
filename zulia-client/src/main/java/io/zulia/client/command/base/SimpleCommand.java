package io.zulia.client.command.base;

import io.zulia.client.result.Result;

public abstract class SimpleCommand<S, R extends Result> extends GrpcCommand<R> {

    public abstract S getRequest();

}
