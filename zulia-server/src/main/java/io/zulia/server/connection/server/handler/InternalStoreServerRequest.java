package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.StoreRequest;
import io.zulia.message.ZuliaServiceOuterClass.StoreResponse;
import io.zulia.server.index.ZuliaIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InternalStoreServerRequest extends ServerRequestHandler<StoreResponse, StoreRequest> {

	private final static Logger LOG = LoggerFactory.getLogger(InternalStoreServerRequest.class);

	public InternalStoreServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected StoreResponse handleCall(ZuliaIndexManager indexManager, StoreRequest request) throws Exception {
		return indexManager.internalStore(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.error("Failed to handle internal store", e);
	}
}
