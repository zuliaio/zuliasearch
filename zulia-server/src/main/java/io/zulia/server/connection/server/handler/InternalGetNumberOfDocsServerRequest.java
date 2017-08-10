package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.GetNumberOfDocsRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetNumberOfDocsResponse;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class InternalGetNumberOfDocsServerRequest extends ServerRequestHandler<GetNumberOfDocsResponse, GetNumberOfDocsRequest> {

	private final static Logger LOG = Logger.getLogger(InternalGetNumberOfDocsServerRequest.class.getSimpleName());

	public InternalGetNumberOfDocsServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected GetNumberOfDocsResponse handleCall(ZuliaIndexManager indexManager, GetNumberOfDocsRequest request) {
		return indexManager.getNumberOfDocsInternal(request);
	}

	@Override
	protected void onError(Exception e) {
		LOG.log(Level.SEVERE, "Failed to handle internal query", e);
	}
}
