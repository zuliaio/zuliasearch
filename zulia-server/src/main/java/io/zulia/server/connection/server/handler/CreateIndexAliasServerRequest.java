package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.CreateIndexAliasRequest;
import io.zulia.message.ZuliaServiceOuterClass.CreateIndexAliasResponse;
import io.zulia.server.index.ZuliaIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateIndexAliasServerRequest extends ServerRequestHandler<CreateIndexAliasResponse, CreateIndexAliasRequest> {

	private final static Logger LOG = LoggerFactory.getLogger(CreateIndexAliasServerRequest.class.getSimpleName());

	public CreateIndexAliasServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected CreateIndexAliasResponse handleCall(ZuliaIndexManager indexManager, CreateIndexAliasRequest request) throws Exception {
		return indexManager.createIndexAlias(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.error("Failed to handle create or update index alias", e);
	}
}
