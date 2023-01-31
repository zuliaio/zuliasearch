package io.zulia.server.connection.client.handler;

import io.zulia.message.ZuliaServiceOuterClass.OptimizeRequest;
import io.zulia.message.ZuliaServiceOuterClass.OptimizeResponse;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.connection.client.InternalRpcConnection;

public class InternalOptimizeHandler extends InternalRequestHandler<OptimizeResponse, OptimizeRequest> {
    public InternalOptimizeHandler(InternalClient internalClient) {
        super(internalClient);
    }

    @Override
    protected OptimizeResponse getResponse(OptimizeRequest optimizeRequest, InternalRpcConnection rpcConnection) {
        return rpcConnection.getService().internalOptimize(optimizeRequest);
    }

}
