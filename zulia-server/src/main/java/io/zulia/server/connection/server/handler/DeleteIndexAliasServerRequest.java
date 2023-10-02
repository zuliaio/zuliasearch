package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.DeleteIndexAliasRequest;
import io.zulia.message.ZuliaServiceOuterClass.DeleteIndexAliasResponse;
import io.zulia.server.index.ZuliaIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteIndexAliasServerRequest extends ServerRequestHandler<DeleteIndexAliasResponse, DeleteIndexAliasRequest> {

	private final static Logger LOG = LoggerFactory.getLogger(DeleteIndexAliasServerRequest.class);

	public DeleteIndexAliasServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected DeleteIndexAliasResponse handleCall(ZuliaIndexManager indexManager, DeleteIndexAliasRequest request) throws Exception {
		return indexManager.deleteIndexAlias(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.error("Failed to handle delete index alias", e);
	}
}
