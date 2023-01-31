package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.FetchRequest;
import io.zulia.message.ZuliaServiceOuterClass.FetchResponse;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class InternalFetchServerRequest extends ServerRequestHandler<FetchResponse, FetchRequest> {

	private final static Logger LOG = Logger.getLogger(InternalFetchServerRequest.class.getSimpleName());

	public InternalFetchServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected FetchResponse handleCall(ZuliaIndexManager indexManager, FetchRequest request) throws Exception {
		return indexManager.internalFetch(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.log(Level.SEVERE, "Failed to handle internal fetch", e);
	}
}
