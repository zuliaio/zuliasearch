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
	protected GetIndexesResponse handleCall(ZuliaIndexManager indexManager, GetIndexesRequest request) {
		return indexManager.getIndexes(request);
	}

	@Override
	protected void onError(Exception e) {
		LOG.log(Level.SEVERE, "Failed to handle internal query", e);
	}
}
