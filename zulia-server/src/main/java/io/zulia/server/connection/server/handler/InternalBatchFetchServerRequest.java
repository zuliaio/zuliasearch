package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.BatchFetchResponse;
import io.zulia.message.ZuliaServiceOuterClass.FetchResponse;
import io.zulia.message.ZuliaServiceOuterClass.InternalBatchFetchRequest;
import io.zulia.server.index.ZuliaIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class InternalBatchFetchServerRequest extends ServerRequestHandler<BatchFetchResponse, InternalBatchFetchRequest> {

	private final static Logger LOG = LoggerFactory.getLogger(InternalBatchFetchServerRequest.class);

	public InternalBatchFetchServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected BatchFetchResponse handleCall(ZuliaIndexManager indexManager, InternalBatchFetchRequest request) throws Exception {
		List<FetchResponse> responses = indexManager.internalBatchFetch(request);
		return BatchFetchResponse.newBuilder().addAllFetchResponse(responses).build();
	}

	@Override
	protected void onError(Throwable e) {
		LOG.error("Failed to handle internal batch fetch", e);
	}
}
