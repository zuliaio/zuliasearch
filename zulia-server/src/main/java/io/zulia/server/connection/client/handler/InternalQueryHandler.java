package io.zulia.server.connection.client.handler;

import io.zulia.message.ZuliaServiceOuterClass.InternalQueryResponse;
import io.zulia.message.ZuliaServiceOuterClass.QueryRequest;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.connection.client.InternalRpcConnection;

public class InternalQueryHandler extends InternalRequestHandler<InternalQueryResponse, QueryRequest> {
	public InternalQueryHandler(InternalClient internalClient) {
		super(internalClient);
	}

	@Override
	protected InternalQueryResponse getResponse(QueryRequest queryRequest, InternalRpcConnection rpcConnection) {
		return rpcConnection.getService().queryInternal(queryRequest);
	}
}
