package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.InternalCreateOrUpdateIndexRequest;
import io.zulia.message.ZuliaServiceOuterClass.InternalCreateOrUpdateIndexResponse;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class InternalCreateIndexServerRequest extends ServerRequestHandler<InternalCreateOrUpdateIndexResponse, InternalCreateOrUpdateIndexRequest> {

    private final static Logger LOG = Logger.getLogger(InternalCreateIndexServerRequest.class.getSimpleName());

    public InternalCreateIndexServerRequest(ZuliaIndexManager indexManager) {
        super(indexManager);
    }

    @Override
    protected InternalCreateOrUpdateIndexResponse handleCall(ZuliaIndexManager indexManager, InternalCreateOrUpdateIndexRequest request) throws Exception {
        return indexManager.internalCreateOrUpdateIndex(request.getIndexName());
    }

    @Override
    protected void onError(Throwable e) {
        LOG.log(Level.SEVERE, "Failed to handle internal create index", e);
    }
}
