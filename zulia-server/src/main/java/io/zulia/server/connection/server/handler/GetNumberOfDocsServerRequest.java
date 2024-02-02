package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.GetNumberOfDocsRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetNumberOfDocsResponse;
import io.zulia.server.index.ZuliaIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetNumberOfDocsServerRequest extends ServerRequestHandler<GetNumberOfDocsResponse, GetNumberOfDocsRequest> {

	private final static Logger LOG = LoggerFactory.getLogger(GetNumberOfDocsServerRequest.class);

	public GetNumberOfDocsServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected GetNumberOfDocsResponse handleCall(ZuliaIndexManager indexManager, GetNumberOfDocsRequest request) throws Exception {
		return indexManager.getNumberOfDocs(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.error("Failed to handle get number of documents", e);
	}
}
