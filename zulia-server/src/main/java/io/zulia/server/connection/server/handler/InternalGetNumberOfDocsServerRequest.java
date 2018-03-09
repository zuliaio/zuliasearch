package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.GetNumberOfDocsResponse;
import io.zulia.message.ZuliaServiceOuterClass.InternalGetNumberOfDocsRequest;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class InternalGetNumberOfDocsServerRequest extends ServerRequestHandler<GetNumberOfDocsResponse, InternalGetNumberOfDocsRequest> {

	private final static Logger LOG = Logger.getLogger(InternalGetNumberOfDocsServerRequest.class.getSimpleName());

	public InternalGetNumberOfDocsServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected GetNumberOfDocsResponse handleCall(ZuliaIndexManager indexManager, InternalGetNumberOfDocsRequest request) throws Exception {
		return indexManager.getNumberOfDocsInternal(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.log(Level.SEVERE, "Failed to handle internal get number of docs", e);
	}
}
