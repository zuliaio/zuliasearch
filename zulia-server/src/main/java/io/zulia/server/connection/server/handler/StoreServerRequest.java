package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.StoreRequest;
import io.zulia.message.ZuliaServiceOuterClass.StoreResponse;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class StoreServerRequest extends ServerRequestHandler<StoreResponse, StoreRequest> {

    private final static Logger LOG = Logger.getLogger(StoreServerRequest.class.getSimpleName());

    public StoreServerRequest(ZuliaIndexManager indexManager) {
        super(indexManager);
    }

    @Override
    protected StoreResponse handleCall(ZuliaIndexManager indexManager, StoreRequest request) throws Exception {
        return indexManager.store(request);
    }

    @Override
    protected void onError(Throwable e) {
        LOG.log(Level.SEVERE, "Failed to handle store", e);
    }
}
