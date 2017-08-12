package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.BatchFetchRequest;
import io.zulia.message.ZuliaServiceOuterClass.BatchFetchResponse;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class BatchFetchServerRequest extends ServerRequestHandler<BatchFetchResponse, BatchFetchRequest> {

	private final static Logger LOG = Logger.getLogger(BatchFetchServerRequest.class.getSimpleName());

	public BatchFetchServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected BatchFetchResponse handleCall(ZuliaIndexManager indexManager, BatchFetchRequest request) {
		return indexManager.batchFetch(request);
	}

	@Override
	protected void onError(Exception e) {
		LOG.log(Level.SEVERE, "Failed to handle batch fetch", e);
	}
}
