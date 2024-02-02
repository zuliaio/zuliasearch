package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.InternalQueryRequest;
import io.zulia.message.ZuliaServiceOuterClass.InternalQueryResponse;
import io.zulia.server.index.ZuliaIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InternalQueryServerRequest extends ServerRequestHandler<InternalQueryResponse, InternalQueryRequest> {

	private final static Logger LOG = LoggerFactory.getLogger(InternalQueryServerRequest.class);

	public InternalQueryServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected InternalQueryResponse handleCall(ZuliaIndexManager indexManager, InternalQueryRequest request) throws Exception {
		return indexManager.internalQuery(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.error("Failed to handle internal query", e);
	}
}
