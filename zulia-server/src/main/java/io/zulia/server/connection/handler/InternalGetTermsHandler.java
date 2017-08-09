package io.zulia.server.connection.handler;

import io.zulia.message.ZuliaServiceOuterClass.GetTermsRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetTermsResponseInternal;
import io.zulia.server.connection.InternalClient;
import io.zulia.server.connection.InternalRpcConnection;

public class InternalGetTermsHandler extends InternalRequestHandler<GetTermsResponseInternal, GetTermsRequest> {
	public InternalGetTermsHandler(InternalClient internalClient) {
		super(internalClient);
	}

	@Override
	protected GetTermsResponseInternal getResponse(GetTermsRequest getTermsRequest, InternalRpcConnection rpcConnection) {
		return rpcConnection.getService().getTermsInternal(getTermsRequest);
	}

}
