package io.zulia.server.connection.handler;

import io.zulia.server.connection.InternalClient;
import io.zulia.server.connection.InternalRpcConnection;

import static io.zulia.message.ZuliaServiceOuterClass.GetNumberOfDocsRequest;
import static io.zulia.message.ZuliaServiceOuterClass.GetNumberOfDocsResponse;

public class InternalGetNumberOfDocsHandler extends InternalRequestHandler<GetNumberOfDocsResponse, GetNumberOfDocsRequest> {
	public InternalGetNumberOfDocsHandler(InternalClient internalClient) {
		super(internalClient);
	}

	@Override
	protected GetNumberOfDocsResponse getResponse(GetNumberOfDocsRequest getNumberOfDocsRequest, InternalRpcConnection rpcConnection) {
		return rpcConnection.getService().getNumberOfDocs(getNumberOfDocsRequest);
	}

}
