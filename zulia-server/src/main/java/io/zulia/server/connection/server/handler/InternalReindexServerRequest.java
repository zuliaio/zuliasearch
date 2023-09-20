package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass;
import io.zulia.message.ZuliaServiceOuterClass.ReindexResponse;
import io.zulia.server.index.ZuliaIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InternalReindexServerRequest extends ServerRequestHandler<ReindexResponse, ZuliaServiceOuterClass.ReindexRequest> {

	private final static Logger LOG = LoggerFactory.getLogger(InternalReindexServerRequest.class.getSimpleName());

	public InternalReindexServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected ReindexResponse handleCall(ZuliaIndexManager indexManager, ZuliaServiceOuterClass.ReindexRequest request) throws Exception {
		return indexManager.internalReindex(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.error("Failed to handle internal reindex", e);
	}
}
