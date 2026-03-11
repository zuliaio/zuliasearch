package io.zulia.server.connection.client.handler;

import io.zulia.message.ZuliaServiceOuterClass.BatchDeleteResponse;
import io.zulia.message.ZuliaServiceOuterClass.InternalBatchDeleteRequest;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.connection.client.InternalRpcConnection;

public class InternalBatchDeleteHandler extends InternalRequestHandler<BatchDeleteResponse, InternalBatchDeleteRequest> {
	public InternalBatchDeleteHandler(InternalClient internalClient) {
		super(internalClient);
	}

	@Override
	protected BatchDeleteResponse getResponse(InternalBatchDeleteRequest request, InternalRpcConnection rpcConnection) {
		return rpcConnection.getService().internalBatchDelete(request);
	}
}
