package io.zulia.server.connection.handler;

import io.zulia.message.ZuliaServiceOuterClass.ClearRequest;
import io.zulia.message.ZuliaServiceOuterClass.ClearResponse;
import io.zulia.server.connection.InternalClient;
import io.zulia.server.connection.InternalRpcConnection;

public class InternalClearHandler extends InternalRequestHandler<ClearResponse, ClearRequest> {
	public InternalClearHandler(InternalClient internalClient) {
		super(internalClient);
	}

	@Override
	protected ClearResponse getResponse(ClearRequest clearRequest, InternalRpcConnection rpcConnection) {
		return rpcConnection.getService().clearInternal(clearRequest);
	}

}
