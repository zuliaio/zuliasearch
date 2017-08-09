package io.zulia.server.connection.handler;

import io.zulia.message.ZuliaServiceOuterClass.StoreRequest;
import io.zulia.message.ZuliaServiceOuterClass.StoreResponse;
import io.zulia.server.connection.InternalClient;
import io.zulia.server.connection.InternalRpcConnection;

public class InternalStoreHandler extends InternalRequestHandler<StoreResponse, StoreRequest> {
	public InternalStoreHandler(InternalClient internalClient) {
		super(internalClient);
	}

	@Override
	protected StoreResponse getResponse(StoreRequest storeRequest, InternalRpcConnection rpcConnection) {

		return rpcConnection.getService().store(storeRequest);
	}

}
