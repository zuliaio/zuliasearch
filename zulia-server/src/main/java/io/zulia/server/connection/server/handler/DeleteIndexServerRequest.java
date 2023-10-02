package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.DeleteIndexRequest;
import io.zulia.message.ZuliaServiceOuterClass.DeleteIndexResponse;
import io.zulia.server.index.ZuliaIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteIndexServerRequest extends ServerRequestHandler<DeleteIndexResponse, DeleteIndexRequest> {

	private final static Logger LOG = LoggerFactory.getLogger(DeleteIndexServerRequest.class);

	public DeleteIndexServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected DeleteIndexResponse handleCall(ZuliaIndexManager indexManager, DeleteIndexRequest request) throws Exception {
		return indexManager.deleteIndex(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.error("Failed to handle delete index", e);
	}
}
