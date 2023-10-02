package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.DeleteIndexAliasRequest;
import io.zulia.message.ZuliaServiceOuterClass.DeleteIndexAliasResponse;
import io.zulia.server.index.ZuliaIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InternalDeleteIndexAliasServerRequest extends ServerRequestHandler<DeleteIndexAliasResponse, DeleteIndexAliasRequest> {

	private final static Logger LOG = LoggerFactory.getLogger(InternalDeleteIndexAliasServerRequest.class);

	public InternalDeleteIndexAliasServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected DeleteIndexAliasResponse handleCall(ZuliaIndexManager indexManager, DeleteIndexAliasRequest request) {
		return indexManager.internalDeleteIndexAlias(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.error("Failed to handle internal delete index alias", e);
	}
}
