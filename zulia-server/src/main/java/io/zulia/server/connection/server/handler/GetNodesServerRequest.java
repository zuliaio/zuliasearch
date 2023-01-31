package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.GetNodesRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetNodesResponse;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class GetNodesServerRequest extends ServerRequestHandler<GetNodesResponse, GetNodesRequest> {

    private final static Logger LOG = Logger.getLogger(GetNodesServerRequest.class.getSimpleName());

    public GetNodesServerRequest(ZuliaIndexManager indexManager) {
        super(indexManager);
    }

    @Override
    protected GetNodesResponse handleCall(ZuliaIndexManager indexManager, GetNodesRequest request) throws Exception {
        return indexManager.getNodes(request);
    }

    @Override
    protected void onError(Throwable e) {
        LOG.log(Level.SEVERE, "Failed to handle get nodes", e);
    }
}
