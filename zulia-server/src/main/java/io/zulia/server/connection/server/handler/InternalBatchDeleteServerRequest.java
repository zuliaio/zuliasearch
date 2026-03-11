package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.BatchDeleteResponse;
import io.zulia.message.ZuliaServiceOuterClass.DeleteResponse;
import io.zulia.message.ZuliaServiceOuterClass.InternalBatchDeleteRequest;
import io.zulia.server.index.ZuliaIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class InternalBatchDeleteServerRequest extends ServerRequestHandler<BatchDeleteResponse, InternalBatchDeleteRequest> {

	private final static Logger LOG = LoggerFactory.getLogger(InternalBatchDeleteServerRequest.class);

	public InternalBatchDeleteServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected BatchDeleteResponse handleCall(ZuliaIndexManager indexManager, InternalBatchDeleteRequest request) throws Exception {
		List<DeleteResponse> responses = indexManager.internalBatchDelete(request);
		return BatchDeleteResponse.newBuilder().addAllDeleteResponse(responses).build();
	}

	@Override
	protected void onError(Throwable e) {
		LOG.error("Failed to handle internal batch delete", e);
	}
}
