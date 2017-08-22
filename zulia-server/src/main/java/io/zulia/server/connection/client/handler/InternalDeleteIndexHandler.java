package io.zulia.server.connection.client.handler;

import io.zulia.message.ZuliaServiceOuterClass.DeleteIndexRequest;
import io.zulia.message.ZuliaServiceOuterClass.DeleteIndexResponse;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.connection.client.InternalRpcConnection;

public class InternalDeleteIndexHandler extends InternalRequestHandler<DeleteIndexResponse, DeleteIndexRequest> {
	public InternalDeleteIndexHandler(InternalClient internalClient) {
		super(internalClient);
	}

	@Override
	protected DeleteIndexResponse getResponse(DeleteIndexRequest deleteIndexRequest, InternalRpcConnection rpcConnection) {
		return rpcConnection.getService().internalDeleteIndex(deleteIndexRequest);
	}

}
