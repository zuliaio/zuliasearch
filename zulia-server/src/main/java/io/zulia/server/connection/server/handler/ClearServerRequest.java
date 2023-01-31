package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.ClearRequest;
import io.zulia.message.ZuliaServiceOuterClass.ClearResponse;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ClearServerRequest extends ServerRequestHandler<ClearResponse, ClearRequest> {

    private final static Logger LOG = Logger.getLogger(ClearServerRequest.class.getSimpleName());

    public ClearServerRequest(ZuliaIndexManager indexManager) {
        super(indexManager);
    }

    @Override
    protected ClearResponse handleCall(ZuliaIndexManager indexManager, ClearRequest request) throws Exception {
        return indexManager.clear(request);
    }

    @Override
    protected void onError(Throwable e) {
        LOG.log(Level.SEVERE, "Failed to handle clear", e);
    }
}
