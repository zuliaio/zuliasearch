package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesResponse;
import io.zulia.server.index.ZuliaIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetFieldNamesServerRequest extends ServerRequestHandler<GetFieldNamesResponse, GetFieldNamesRequest> {

	private final static Logger LOG = LoggerFactory.getLogger(GetFieldNamesServerRequest.class);

	public GetFieldNamesServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected GetFieldNamesResponse handleCall(ZuliaIndexManager indexManager, GetFieldNamesRequest request) throws Exception {
		return indexManager.getFieldNames(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.error("Failed to handle get field names", e);
	}
}
