package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.CreateIndexResponse;
import io.zulia.message.ZuliaServiceOuterClass.InternalCreateIndexRequest;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class InternalCreateIndexServerRequest extends ServerRequestHandler<CreateIndexResponse, InternalCreateIndexRequest> {

	private final static Logger LOG = Logger.getLogger(InternalCreateIndexServerRequest.class.getSimpleName());

	public InternalCreateIndexServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected CreateIndexResponse handleCall(ZuliaIndexManager indexManager, InternalCreateIndexRequest request) throws Exception {
		return indexManager.internalCreateIndex(request.getIndexName());
	}

	@Override
	protected void onError(Exception e) {
		LOG.log(Level.SEVERE, "Failed to handle internal create index", e);
	}
}
