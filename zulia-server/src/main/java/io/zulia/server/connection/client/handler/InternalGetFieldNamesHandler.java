package io.zulia.server.connection.client.handler;

import io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesResponse;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.connection.client.InternalRpcConnection;

public class InternalGetFieldNamesHandler extends InternalRequestHandler<GetFieldNamesResponse, GetFieldNamesRequest> {
	public InternalGetFieldNamesHandler(InternalClient internalClient) {
		super(internalClient);
	}

	@Override
	protected GetFieldNamesResponse getResponse(GetFieldNamesRequest getFieldNamesRequest, InternalRpcConnection rpcConnection) {
		return rpcConnection.getService().getFieldNamesInternal(getFieldNamesRequest);
	}

}
