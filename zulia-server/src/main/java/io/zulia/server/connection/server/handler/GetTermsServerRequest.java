package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.GetTermsRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetTermsResponse;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class GetTermsServerRequest extends ServerRequestHandler<GetTermsResponse, GetTermsRequest> {

    private final static Logger LOG = Logger.getLogger(GetTermsServerRequest.class.getSimpleName());

    public GetTermsServerRequest(ZuliaIndexManager indexManager) {
        super(indexManager);
    }

    @Override
    protected GetTermsResponse handleCall(ZuliaIndexManager indexManager, GetTermsRequest request) throws Exception {
        return indexManager.getTerms(request);
    }

    @Override
    protected void onError(Throwable e) {
        LOG.log(Level.SEVERE, "Failed to handle get terms request", e);
    }
}
