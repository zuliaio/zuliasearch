package io.zulia.server.connection.client.handler;

import io.zulia.message.ZuliaServiceOuterClass.DeleteIndexAliasRequest;
import io.zulia.message.ZuliaServiceOuterClass.DeleteIndexAliasResponse;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.connection.client.InternalRpcConnection;

public class InternalDeleteIndexAliasHandler extends InternalRequestHandler<DeleteIndexAliasResponse, DeleteIndexAliasRequest> {
    public InternalDeleteIndexAliasHandler(InternalClient internalClient) {
        super(internalClient);
    }

    @Override
    protected DeleteIndexAliasResponse getResponse(DeleteIndexAliasRequest deleteIndexAliasRequest, InternalRpcConnection rpcConnection) {
        return rpcConnection.getService().internalDeleteIndexAlias(deleteIndexAliasRequest);
    }

}
