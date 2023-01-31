package io.zulia.server.connection.client.handler;

import io.zulia.message.ZuliaServiceOuterClass;
import io.zulia.message.ZuliaServiceOuterClass.CreateIndexAliasResponse;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.connection.client.InternalRpcConnection;

public class InternalCreateIndexAliasHandler extends InternalRequestHandler<CreateIndexAliasResponse, ZuliaServiceOuterClass.InternalCreateIndexAliasRequest> {
    public InternalCreateIndexAliasHandler(InternalClient internalClient) {
        super(internalClient);
    }

    @Override
    protected CreateIndexAliasResponse getResponse(ZuliaServiceOuterClass.InternalCreateIndexAliasRequest internalCreateIndexAliasRequest,
                                                   InternalRpcConnection rpcConnection) {
        return rpcConnection.getService().internalCreateIndexAlias(internalCreateIndexAliasRequest);
    }

}
