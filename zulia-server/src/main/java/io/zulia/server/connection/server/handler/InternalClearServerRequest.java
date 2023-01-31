package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.ClearRequest;
import io.zulia.message.ZuliaServiceOuterClass.ClearResponse;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class InternalClearServerRequest extends ServerRequestHandler<ClearResponse, ClearRequest> {

    private final static Logger LOG = Logger.getLogger(InternalClearServerRequest.class.getSimpleName());

    public InternalClearServerRequest(ZuliaIndexManager indexManager) {
        super(indexManager);
    }

    @Override
    protected ClearResponse handleCall(ZuliaIndexManager indexManager, ClearRequest request) throws Exception {
        return indexManager.internalClear(request);
    }

    @Override
    protected void onError(Throwable e) {
        LOG.log(Level.SEVERE, "Failed to handle internal clear", e);
    }
}
