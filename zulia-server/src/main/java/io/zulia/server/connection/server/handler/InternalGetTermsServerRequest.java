package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.InternalGetTermsRequest;
import io.zulia.message.ZuliaServiceOuterClass.InternalGetTermsResponse;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class InternalGetTermsServerRequest extends ServerRequestHandler<InternalGetTermsResponse, InternalGetTermsRequest> {

    private final static Logger LOG = Logger.getLogger(InternalGetTermsServerRequest.class.getSimpleName());

    public InternalGetTermsServerRequest(ZuliaIndexManager indexManager) {
        super(indexManager);
    }

    @Override
    protected InternalGetTermsResponse handleCall(ZuliaIndexManager indexManager, InternalGetTermsRequest request) throws Exception {
        return indexManager.internalGetTerms(request);
    }

    @Override
    protected void onError(Throwable e) {
        LOG.log(Level.SEVERE, "Failed to handle internal get terms", e);
    }
}
