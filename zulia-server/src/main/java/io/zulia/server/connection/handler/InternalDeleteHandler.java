package io.zulia.server.connection.handler;

import io.zulia.message.ZuliaServiceOuterClass.DeleteRequest;
import io.zulia.message.ZuliaServiceOuterClass.DeleteResponse;
import io.zulia.server.connection.InternalClient;
import io.zulia.server.connection.InternalRpcConnection;

public class InternalDeleteHandler extends InternalRequestHandler<DeleteResponse, DeleteRequest> {
	public InternalDeleteHandler(InternalClient internalClient) {
		super(internalClient);
	}

	@Override
	protected DeleteResponse getResponse(DeleteRequest deleteRequest, InternalRpcConnection rpcConnection) {
		return rpcConnection.getService().delete(deleteRequest);
	}

}
