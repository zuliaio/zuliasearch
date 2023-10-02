package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.GetTermsRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetTermsResponse;
import io.zulia.server.index.ZuliaIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetTermsServerRequest extends ServerRequestHandler<GetTermsResponse, GetTermsRequest> {

	private final static Logger LOG = LoggerFactory.getLogger(GetTermsServerRequest.class);

	public GetTermsServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected GetTermsResponse handleCall(ZuliaIndexManager indexManager, GetTermsRequest request) throws Exception {
		return indexManager.getTerms(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.error("Failed to handle get terms request", e);
	}
}
