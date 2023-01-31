package io.zulia.server.connection.client.handler;

import io.zulia.message.ZuliaServiceOuterClass;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.connection.client.InternalRpcConnection;

public class InternalFetchHandler extends InternalRequestHandler<ZuliaServiceOuterClass.FetchResponse, ZuliaServiceOuterClass.FetchRequest> {
	public InternalFetchHandler(InternalClient internalClient) {
		super(internalClient);
	}

	@Override
	protected ZuliaServiceOuterClass.FetchResponse getResponse(ZuliaServiceOuterClass.FetchRequest fetchRequest, InternalRpcConnection rpcConnection) {
		return rpcConnection.getService().internalFetch(fetchRequest);
	}
}
