package io.zulia.server.connection.client.handler;

import io.zulia.message.ZuliaServiceOuterClass.CreateIndexResponse;
import io.zulia.message.ZuliaServiceOuterClass.InternalCreateIndexRequest;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.connection.client.InternalRpcConnection;

public class InternalCreateIndexHandler extends InternalRequestHandler<CreateIndexResponse, InternalCreateIndexRequest> {
	public InternalCreateIndexHandler(InternalClient internalClient) {
		super(internalClient);
	}

	@Override
	protected CreateIndexResponse getResponse(InternalCreateIndexRequest createIndexRequest, InternalRpcConnection rpcConnection) {
		return rpcConnection.getService().internalCreateIndex(createIndexRequest);
	}

}
