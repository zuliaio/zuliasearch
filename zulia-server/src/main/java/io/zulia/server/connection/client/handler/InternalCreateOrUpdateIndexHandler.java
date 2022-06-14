package io.zulia.server.connection.client.handler;

import io.zulia.message.ZuliaServiceOuterClass.InternalCreateOrUpdateIndexRequest;
import io.zulia.message.ZuliaServiceOuterClass.InternalCreateOrUpdateIndexResponse;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.connection.client.InternalRpcConnection;

public class InternalCreateOrUpdateIndexHandler
		extends InternalRequestHandler<InternalCreateOrUpdateIndexResponse, InternalCreateOrUpdateIndexRequest> {
	public InternalCreateOrUpdateIndexHandler(InternalClient internalClient) {
		super(internalClient);
	}

	@Override
	protected InternalCreateOrUpdateIndexResponse getResponse(InternalCreateOrUpdateIndexRequest internalCreateOrUpdateIndexRequest,
			InternalRpcConnection rpcConnection) {
		return rpcConnection.getService().internalCreateOrUpdateIndex(internalCreateOrUpdateIndexRequest);
	}

}
