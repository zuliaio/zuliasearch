package io.zulia.server.connection.client.handler;

import io.zulia.message.ZuliaServiceOuterClass.CreateIndexRequest;
import io.zulia.message.ZuliaServiceOuterClass.CreateIndexResponse;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.connection.client.InternalRpcConnection;

public class InternalCreateIndexHandler extends InternalRequestHandler<CreateIndexResponse, CreateIndexRequest> {
	public InternalCreateIndexHandler(InternalClient internalClient) {
		super(internalClient);
	}

	@Override
	protected CreateIndexResponse getResponse(CreateIndexRequest createIndexRequest, InternalRpcConnection rpcConnection) {
		return rpcConnection.getService().internalCreateIndex(createIndexRequest);
	}

}
