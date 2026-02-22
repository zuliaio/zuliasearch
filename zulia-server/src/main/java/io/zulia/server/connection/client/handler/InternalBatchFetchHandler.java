package io.zulia.server.connection.client.handler;

import io.zulia.message.ZuliaServiceOuterClass.BatchFetchResponse;
import io.zulia.message.ZuliaServiceOuterClass.InternalBatchFetchRequest;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.connection.client.InternalRpcConnection;

public class InternalBatchFetchHandler extends InternalRequestHandler<BatchFetchResponse, InternalBatchFetchRequest> {
	public InternalBatchFetchHandler(InternalClient internalClient) {
		super(internalClient);
	}

	@Override
	protected BatchFetchResponse getResponse(InternalBatchFetchRequest request, InternalRpcConnection rpcConnection) {
		return rpcConnection.getService().internalBatchFetch(request);
	}
}
