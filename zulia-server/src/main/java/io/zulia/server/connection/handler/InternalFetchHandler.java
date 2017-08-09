package io.zulia.server.connection.handler;

import io.zulia.message.ZuliaServiceOuterClass;
import io.zulia.server.connection.InternalClient;
import io.zulia.server.connection.InternalRpcConnection;

public class InternalFetchHandler extends InternalRequestHandler<ZuliaServiceOuterClass.FetchResponse, ZuliaServiceOuterClass.FetchRequest> {
	public InternalFetchHandler(InternalClient internalClient) {
		super(internalClient);
	}

	@Override
	protected ZuliaServiceOuterClass.FetchResponse getResponse(ZuliaServiceOuterClass.FetchRequest fetchRequest, InternalRpcConnection rpcConnection) {
		return rpcConnection.getService().fetch(fetchRequest);
	}
}
