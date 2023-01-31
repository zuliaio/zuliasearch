package io.zulia.server.connection.client.handler;

import io.zulia.message.ZuliaServiceOuterClass.InternalGetTermsRequest;
import io.zulia.message.ZuliaServiceOuterClass.InternalGetTermsResponse;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.connection.client.InternalRpcConnection;

public class InternalGetTermsHandler extends InternalRequestHandler<InternalGetTermsResponse, InternalGetTermsRequest> {
    public InternalGetTermsHandler(InternalClient internalClient) {
        super(internalClient);
    }

    @Override
    protected InternalGetTermsResponse getResponse(InternalGetTermsRequest getTermsRequest, InternalRpcConnection rpcConnection) {
        return rpcConnection.getService().internalGetTerms(getTermsRequest);
    }

}
