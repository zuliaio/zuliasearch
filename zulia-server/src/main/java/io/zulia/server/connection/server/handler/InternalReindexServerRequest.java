package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass;
import io.zulia.message.ZuliaServiceOuterClass.ReindexResponse;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class InternalReindexServerRequest extends ServerRequestHandler<ReindexResponse, ZuliaServiceOuterClass.ReindexRequest> {

	private final static Logger LOG = Logger.getLogger(InternalReindexServerRequest.class.getSimpleName());

	public InternalReindexServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected ReindexResponse handleCall(ZuliaIndexManager indexManager, ZuliaServiceOuterClass.ReindexRequest request) throws Exception {
		return indexManager.internalReindex(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.log(Level.SEVERE, "Failed to handle internal reindex", e);
	}
}
