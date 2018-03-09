package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.GetIndexesRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetIndexesResponse;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class GetIndexesServerRequest extends ServerRequestHandler<GetIndexesResponse, GetIndexesRequest> {

	private final static Logger LOG = Logger.getLogger(GetIndexesServerRequest.class.getSimpleName());

	public GetIndexesServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected GetIndexesResponse handleCall(ZuliaIndexManager indexManager, GetIndexesRequest request) throws Exception {
		return indexManager.getIndexes(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.log(Level.SEVERE, "Failed to handle get indexes request", e);
	}
}
