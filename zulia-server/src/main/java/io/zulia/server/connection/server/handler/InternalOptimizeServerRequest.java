package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.OptimizeRequest;
import io.zulia.message.ZuliaServiceOuterClass.OptimizeResponse;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class InternalOptimizeServerRequest extends ServerRequestHandler<OptimizeResponse, OptimizeRequest> {

	private final static Logger LOG = Logger.getLogger(InternalOptimizeServerRequest.class.getSimpleName());

	public InternalOptimizeServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected OptimizeResponse handleCall(ZuliaIndexManager indexManager, OptimizeRequest request) {
		return indexManager.internalOptimize(request);
	}

	@Override
	protected void onError(Exception e) {
		LOG.log(Level.SEVERE, "Failed to handle internal query", e);
	}
}
