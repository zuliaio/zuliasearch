package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.DeleteRequest;
import io.zulia.message.ZuliaServiceOuterClass.DeleteResponse;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class InternalDeleteServerRequest extends ServerRequestHandler<DeleteResponse, DeleteRequest> {

	private final static Logger LOG = Logger.getLogger(InternalDeleteServerRequest.class.getSimpleName());

	public InternalDeleteServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected DeleteResponse handleCall(ZuliaIndexManager indexManager, DeleteRequest request) throws Exception {
		return indexManager.internalDelete(request);
	}

	@Override
	protected void onError(Exception e) {
		LOG.log(Level.SEVERE, "Failed to handle internal delete", e);
	}
}
