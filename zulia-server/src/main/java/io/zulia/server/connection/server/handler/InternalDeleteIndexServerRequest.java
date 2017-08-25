package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.DeleteIndexRequest;
import io.zulia.message.ZuliaServiceOuterClass.DeleteIndexResponse;
import io.zulia.server.index.ZuliaIndexManager;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class InternalDeleteIndexServerRequest extends ServerRequestHandler<DeleteIndexResponse, DeleteIndexRequest> {

	private final static Logger LOG = Logger.getLogger(InternalDeleteIndexServerRequest.class.getSimpleName());

	public InternalDeleteIndexServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected DeleteIndexResponse handleCall(ZuliaIndexManager indexManager, DeleteIndexRequest request) throws IOException {
		return indexManager.internalDeleteIndex(request);
	}

	@Override
	protected void onError(Exception e) {
		LOG.log(Level.SEVERE, "Failed to handle internal delete index", e);
	}
}
