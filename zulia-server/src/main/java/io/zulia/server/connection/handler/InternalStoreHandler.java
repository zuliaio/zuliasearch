package io.zulia.server.connection.handler;

import io.zulia.message.ZuliaServiceOuterClass;
import io.zulia.server.connection.InternalClient;
import io.zulia.server.connection.InternalRpcConnection;

public class InternalStoreHandler extends InternalRequestHandler<ZuliaServiceOuterClass.StoreResponse, ZuliaServiceOuterClass.StoreRequest> {
	public InternalStoreHandler(InternalClient internalClient) {
		super(internalClient);
	}

	@Override
	protected ZuliaServiceOuterClass.StoreResponse getResponse(ZuliaServiceOuterClass.StoreRequest storeRequest, InternalRpcConnection rpcConnection) {

		return rpcConnection.getService().store(storeRequest);
	}

}
