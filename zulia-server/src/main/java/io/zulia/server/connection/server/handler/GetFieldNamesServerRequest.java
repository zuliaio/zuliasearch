package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesResponse;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class GetFieldNamesServerRequest extends ServerRequestHandler<GetFieldNamesResponse, GetFieldNamesRequest> {

	private final static Logger LOG = Logger.getLogger(GetFieldNamesServerRequest.class.getSimpleName());

	public GetFieldNamesServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected GetFieldNamesResponse handleCall(ZuliaIndexManager indexManager, GetFieldNamesRequest request) throws Exception {
		return indexManager.getFieldNames(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.log(Level.SEVERE, "Failed to handle get field names", e);
	}
}
