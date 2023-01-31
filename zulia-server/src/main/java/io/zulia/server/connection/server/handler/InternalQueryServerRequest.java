package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.InternalQueryRequest;
import io.zulia.message.ZuliaServiceOuterClass.InternalQueryResponse;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class InternalQueryServerRequest extends ServerRequestHandler<InternalQueryResponse, InternalQueryRequest> {

    private final static Logger LOG = Logger.getLogger(InternalQueryServerRequest.class.getSimpleName());

    public InternalQueryServerRequest(ZuliaIndexManager indexManager) {
        super(indexManager);
    }

    @Override
    protected InternalQueryResponse handleCall(ZuliaIndexManager indexManager, InternalQueryRequest request) throws Exception {
        return indexManager.internalQuery(request);
    }

    @Override
    protected void onError(Throwable e) {
        LOG.log(Level.SEVERE, "Failed to handle internal query", e);
    }
}
