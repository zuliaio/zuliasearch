package io.zulia.server.connection.client.handler;

import io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesResponse;
import io.zulia.message.ZuliaServiceOuterClass.InternalGetFieldNamesRequest;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.connection.client.InternalRpcConnection;

public class InternalGetFieldNamesHandler extends InternalRequestHandler<GetFieldNamesResponse, InternalGetFieldNamesRequest> {
	public InternalGetFieldNamesHandler(InternalClient internalClient) {
		super(internalClient);
	}

	@Override
	protected GetFieldNamesResponse getResponse(InternalGetFieldNamesRequest getFieldNamesRequest, InternalRpcConnection rpcConnection) {
		return rpcConnection.getService().internalGetFieldNames(getFieldNamesRequest);
	}

}
