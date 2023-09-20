package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesResponse;
import io.zulia.message.ZuliaServiceOuterClass.InternalGetFieldNamesRequest;
import io.zulia.server.index.ZuliaIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InternalGetFieldNamesServerRequest extends ServerRequestHandler<GetFieldNamesResponse, InternalGetFieldNamesRequest> {

	private final static Logger LOG = LoggerFactory.getLogger(InternalGetFieldNamesServerRequest.class.getSimpleName());

	public InternalGetFieldNamesServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected GetFieldNamesResponse handleCall(ZuliaIndexManager indexManager, InternalGetFieldNamesRequest request) throws Exception {
		return indexManager.internalGetFieldNames(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.error("Failed to handle internal get field names", e);
	}
}
