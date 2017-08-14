package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.BatchDeleteRequest;
import io.zulia.message.ZuliaServiceOuterClass.BatchDeleteResponse;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class BatchDeleteServerRequest extends ServerRequestHandler<BatchDeleteResponse, BatchDeleteRequest> {

	private final static Logger LOG = Logger.getLogger(BatchDeleteServerRequest.class.getSimpleName());

	public BatchDeleteServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected BatchDeleteResponse handleCall(ZuliaIndexManager indexManager, BatchDeleteRequest request) throws Exception {
		return indexManager.batchDelete(request);
	}

	@Override
	protected void onError(Exception e) {
		LOG.log(Level.SEVERE, "Failed to handle batch delete", e);
	}
}
