package io.zulia.server.connection.client.handler;

import io.zulia.message.ZuliaServiceOuterClass.InternalGetNumberOfDocsRequest;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.connection.client.InternalRpcConnection;

import static io.zulia.message.ZuliaServiceOuterClass.GetNumberOfDocsResponse;

public class InternalGetNumberOfDocsHandler extends InternalRequestHandler<GetNumberOfDocsResponse, InternalGetNumberOfDocsRequest> {
	public InternalGetNumberOfDocsHandler(InternalClient internalClient) {
		super(internalClient);
	}

	@Override
	protected GetNumberOfDocsResponse getResponse(InternalGetNumberOfDocsRequest getNumberOfDocsRequest, InternalRpcConnection rpcConnection) {
		return rpcConnection.getService().internalGetNumberOfDocs(getNumberOfDocsRequest);
	}

}
