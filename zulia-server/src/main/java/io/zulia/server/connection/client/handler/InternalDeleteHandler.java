package io.zulia.server.connection.client.handler;

import io.zulia.message.ZuliaServiceOuterClass.DeleteRequest;
import io.zulia.message.ZuliaServiceOuterClass.DeleteResponse;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.connection.client.InternalRpcConnection;

public class InternalDeleteHandler extends InternalRequestHandler<DeleteResponse, DeleteRequest> {
	public InternalDeleteHandler(InternalClient internalClient) {
		super(internalClient);
	}

	@Override
	protected DeleteResponse getResponse(DeleteRequest deleteRequest, InternalRpcConnection rpcConnection) {
		return rpcConnection.getService().delete(deleteRequest);
	}

}
