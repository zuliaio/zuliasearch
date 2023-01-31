package io.zulia.server.connection.client.handler;

import io.zulia.message.ZuliaServiceOuterClass.ReindexRequest;
import io.zulia.message.ZuliaServiceOuterClass.ReindexResponse;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.connection.client.InternalRpcConnection;

public class InternalReindexHandler extends InternalRequestHandler<ReindexResponse, ReindexRequest> {
	public InternalReindexHandler(InternalClient internalClient) {
		super(internalClient);
	}

	@Override
	protected ReindexResponse getResponse(ReindexRequest reindexRequest, InternalRpcConnection rpcConnection) {
		return rpcConnection.getService().internalReindex(reindexRequest);
	}

}
