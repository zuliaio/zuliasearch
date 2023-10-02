package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.ReindexRequest;
import io.zulia.message.ZuliaServiceOuterClass.ReindexResponse;
import io.zulia.server.index.ZuliaIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReindexServerRequest extends ServerRequestHandler<ReindexResponse, ReindexRequest> {

	private final static Logger LOG = LoggerFactory.getLogger(ReindexServerRequest.class);

	public ReindexServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected ReindexResponse handleCall(ZuliaIndexManager indexManager, ReindexRequest request) throws Exception {
		return indexManager.reindex(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.error("Failed to handle reindex", e);
	}
}
