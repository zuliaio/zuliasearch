package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.DeleteRequest;
import io.zulia.message.ZuliaServiceOuterClass.DeleteResponse;
import io.zulia.server.index.ZuliaIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InternalDeleteServerRequest extends ServerRequestHandler<DeleteResponse, DeleteRequest> {

	private final static Logger LOG = LoggerFactory.getLogger(InternalDeleteServerRequest.class);

	public InternalDeleteServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected DeleteResponse handleCall(ZuliaIndexManager indexManager, DeleteRequest request) throws Exception {
		return indexManager.internalDelete(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.error("Failed to handle internal delete", e);
	}
}
