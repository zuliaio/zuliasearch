package io.zulia.server.connection.client.handler;

import io.zulia.message.ZuliaServiceOuterClass.GetTermsRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetTermsResponseInternal;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.connection.client.InternalRpcConnection;

public class InternalGetTermsHandler extends InternalRequestHandler<GetTermsResponseInternal, GetTermsRequest> {
	public InternalGetTermsHandler(InternalClient internalClient) {
		super(internalClient);
	}

	@Override
	protected GetTermsResponseInternal getResponse(GetTermsRequest getTermsRequest, InternalRpcConnection rpcConnection) {
		return rpcConnection.getService().getTermsInternal(getTermsRequest);
	}

}
