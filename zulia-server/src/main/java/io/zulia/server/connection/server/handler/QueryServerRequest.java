package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.QueryRequest;
import io.zulia.message.ZuliaServiceOuterClass.QueryResponse;
import io.zulia.server.index.ZuliaIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryServerRequest extends ServerRequestHandler<QueryResponse, QueryRequest> {

	private final static Logger LOG = LoggerFactory.getLogger(QueryServerRequest.class.getSimpleName());

	public QueryServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected QueryResponse handleCall(ZuliaIndexManager indexManager, QueryRequest request) throws Exception {
		return indexManager.query(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.error("Failed to handle query", e);
	}
}
