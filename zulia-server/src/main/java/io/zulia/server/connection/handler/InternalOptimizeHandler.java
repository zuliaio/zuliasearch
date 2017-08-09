package io.zulia.server.connection.handler;

import io.zulia.message.ZuliaServiceOuterClass.OptimizeRequest;
import io.zulia.message.ZuliaServiceOuterClass.OptimizeResponse;
import io.zulia.server.connection.InternalClient;
import io.zulia.server.connection.InternalRpcConnection;

public class InternalOptimizeHandler extends InternalRequestHandler<OptimizeResponse, OptimizeRequest> {
	public InternalOptimizeHandler(InternalClient internalClient) {
		super(internalClient);
	}

	@Override
	protected OptimizeResponse getResponse(OptimizeRequest optimizeRequest, InternalRpcConnection rpcConnection) {
		return rpcConnection.getService().optimizeInternal(optimizeRequest);
	}

}
