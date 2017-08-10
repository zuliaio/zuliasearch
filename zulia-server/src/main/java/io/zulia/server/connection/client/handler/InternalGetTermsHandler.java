package io.zulia.server.connection.client.handler;

import io.zulia.message.ZuliaServiceOuterClass.GetTermsRequest;
import io.zulia.message.ZuliaServiceOuterClass.InternalGetTermsResponse;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.connection.client.InternalRpcConnection;

public class InternalGetTermsHandler extends InternalRequestHandler<InternalGetTermsResponse, GetTermsRequest> {
	public InternalGetTermsHandler(InternalClient internalClient) {
		super(internalClient);
	}

	@Override
	protected InternalGetTermsResponse getResponse(GetTermsRequest getTermsRequest, InternalRpcConnection rpcConnection) {
		return rpcConnection.getService().internalGetTerms(getTermsRequest);
	}

}
