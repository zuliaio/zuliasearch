package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.CreateIndexRequest;
import io.zulia.message.ZuliaServiceOuterClass.CreateIndexResponse;
import io.zulia.server.index.ZuliaIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateIndexServerRequest extends ServerRequestHandler<CreateIndexResponse, CreateIndexRequest> {

	private final static Logger LOG = LoggerFactory.getLogger(CreateIndexServerRequest.class);

	public CreateIndexServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected CreateIndexResponse handleCall(ZuliaIndexManager indexManager, CreateIndexRequest request) throws Exception {
		return indexManager.createIndex(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.error("Failed to handle create index", e);
	}
}
