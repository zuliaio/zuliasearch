package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.InternalGetTermsRequest;
import io.zulia.message.ZuliaServiceOuterClass.InternalGetTermsResponse;
import io.zulia.server.index.ZuliaIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InternalGetTermsServerRequest extends ServerRequestHandler<InternalGetTermsResponse, InternalGetTermsRequest> {

	private final static Logger LOG = LoggerFactory.getLogger(InternalGetTermsServerRequest.class.getSimpleName());

	public InternalGetTermsServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected InternalGetTermsResponse handleCall(ZuliaIndexManager indexManager, InternalGetTermsRequest request) throws Exception {
		return indexManager.internalGetTerms(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.error("Failed to handle internal get terms", e);
	}
}
