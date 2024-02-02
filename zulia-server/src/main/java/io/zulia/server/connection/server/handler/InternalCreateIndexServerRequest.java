package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.InternalCreateOrUpdateIndexRequest;
import io.zulia.message.ZuliaServiceOuterClass.InternalCreateOrUpdateIndexResponse;
import io.zulia.server.index.ZuliaIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InternalCreateIndexServerRequest extends ServerRequestHandler<InternalCreateOrUpdateIndexResponse, InternalCreateOrUpdateIndexRequest> {

	private final static Logger LOG = LoggerFactory.getLogger(InternalCreateIndexServerRequest.class);

	public InternalCreateIndexServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected InternalCreateOrUpdateIndexResponse handleCall(ZuliaIndexManager indexManager, InternalCreateOrUpdateIndexRequest request) throws Exception {
		return indexManager.internalCreateOrUpdateIndex(request.getIndexName());
	}

	@Override
	protected void onError(Throwable e) {
		LOG.error("Failed to handle internal create index", e);
	}
}
