package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.ReindexRequest;
import io.zulia.message.ZuliaServiceOuterClass.ReindexResponse;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ReindexServerRequest extends ServerRequestHandler<ReindexResponse, ReindexRequest> {

	private final static Logger LOG = Logger.getLogger(ReindexServerRequest.class.getSimpleName());

	public ReindexServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected ReindexResponse handleCall(ZuliaIndexManager indexManager, ReindexRequest request) throws Exception {
		return indexManager.reindex(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.log(Level.SEVERE, "Failed to handle reindex", e);
	}
}
