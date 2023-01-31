package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.UpdateIndexRequest;
import io.zulia.message.ZuliaServiceOuterClass.UpdateIndexResponse;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class UpdateIndexServerRequest extends ServerRequestHandler<UpdateIndexResponse, UpdateIndexRequest> {

	private final static Logger LOG = Logger.getLogger(UpdateIndexServerRequest.class.getSimpleName());

	public UpdateIndexServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected UpdateIndexResponse handleCall(ZuliaIndexManager indexManager, UpdateIndexRequest request) throws Exception {
		return indexManager.updateIndex(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.log(Level.SEVERE, "Failed to handle create index", e);
	}
}
