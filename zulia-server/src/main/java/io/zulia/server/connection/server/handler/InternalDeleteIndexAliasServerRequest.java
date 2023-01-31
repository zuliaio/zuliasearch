package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.DeleteIndexAliasRequest;
import io.zulia.message.ZuliaServiceOuterClass.DeleteIndexAliasResponse;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class InternalDeleteIndexAliasServerRequest extends ServerRequestHandler<DeleteIndexAliasResponse, DeleteIndexAliasRequest> {

	private final static Logger LOG = Logger.getLogger(InternalDeleteIndexAliasServerRequest.class.getSimpleName());

	public InternalDeleteIndexAliasServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected DeleteIndexAliasResponse handleCall(ZuliaIndexManager indexManager, DeleteIndexAliasRequest request) throws Exception {
		return indexManager.internalDeleteIndexAlias(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.log(Level.SEVERE, "Failed to handle internal delete index alias", e);
	}
}
