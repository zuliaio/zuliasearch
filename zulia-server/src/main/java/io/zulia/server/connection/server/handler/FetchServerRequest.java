package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.FetchRequest;
import io.zulia.message.ZuliaServiceOuterClass.FetchResponse;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class FetchServerRequest extends ServerRequestHandler<FetchResponse, FetchRequest> {

	private final static Logger LOG = Logger.getLogger(FetchServerRequest.class.getSimpleName());

	public FetchServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected FetchResponse handleCall(ZuliaIndexManager indexManager, FetchRequest request) {
		return indexManager.fetch(request);
	}

	@Override
	protected void onError(Exception e) {
		LOG.log(Level.SEVERE, "Failed to handle internal query", e);
	}
}
