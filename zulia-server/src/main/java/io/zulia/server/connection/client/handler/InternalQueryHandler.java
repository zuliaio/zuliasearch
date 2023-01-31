package io.zulia.server.connection.client.handler;

import io.zulia.message.ZuliaServiceOuterClass.InternalQueryRequest;
import io.zulia.message.ZuliaServiceOuterClass.InternalQueryResponse;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.connection.client.InternalRpcConnection;

public class InternalQueryHandler extends InternalRequestHandler<InternalQueryResponse, InternalQueryRequest> {
    public InternalQueryHandler(InternalClient internalClient) {
        super(internalClient);
    }

    @Override
    protected InternalQueryResponse getResponse(InternalQueryRequest queryRequest, InternalRpcConnection rpcConnection) {
        return rpcConnection.getService().internalQuery(queryRequest);
    }
}
