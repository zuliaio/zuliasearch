package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.UpdateIndexRequest;
import io.zulia.message.ZuliaServiceOuterClass.UpdateIndexResponse;
import io.zulia.server.index.ZuliaIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateIndexServerRequest extends ServerRequestHandler<UpdateIndexResponse, UpdateIndexRequest> {

	private final static Logger LOG = LoggerFactory.getLogger(UpdateIndexServerRequest.class);

	public UpdateIndexServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected UpdateIndexResponse handleCall(ZuliaIndexManager indexManager, UpdateIndexRequest request) throws Exception {
		return indexManager.updateIndex(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.error("Failed to handle create index", e);
	}
}
