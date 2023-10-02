package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.ClearRequest;
import io.zulia.message.ZuliaServiceOuterClass.ClearResponse;
import io.zulia.server.index.ZuliaIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClearServerRequest extends ServerRequestHandler<ClearResponse, ClearRequest> {

	private final static Logger LOG = LoggerFactory.getLogger(ClearServerRequest.class);

	public ClearServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected ClearResponse handleCall(ZuliaIndexManager indexManager, ClearRequest request) throws Exception {
		return indexManager.clear(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.error("Failed to handle clear", e);
	}
}
