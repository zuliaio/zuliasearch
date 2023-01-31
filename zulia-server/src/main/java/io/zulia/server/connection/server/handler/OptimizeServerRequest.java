package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.OptimizeRequest;
import io.zulia.message.ZuliaServiceOuterClass.OptimizeResponse;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class OptimizeServerRequest extends ServerRequestHandler<OptimizeResponse, OptimizeRequest> {

    private final static Logger LOG = Logger.getLogger(OptimizeServerRequest.class.getSimpleName());

    public OptimizeServerRequest(ZuliaIndexManager indexManager) {
        super(indexManager);
    }

    @Override
    protected OptimizeResponse handleCall(ZuliaIndexManager indexManager, OptimizeRequest request) throws Exception {
        return indexManager.optimize(request);
    }

    @Override
    protected void onError(Throwable e) {
        LOG.log(Level.SEVERE, "Failed to handle optimize", e);
    }
}
