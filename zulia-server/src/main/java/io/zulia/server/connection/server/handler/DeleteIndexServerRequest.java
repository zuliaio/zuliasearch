package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.DeleteIndexRequest;
import io.zulia.message.ZuliaServiceOuterClass.DeleteIndexResponse;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class DeleteIndexServerRequest extends ServerRequestHandler<DeleteIndexResponse, DeleteIndexRequest> {

    private final static Logger LOG = Logger.getLogger(DeleteIndexServerRequest.class.getSimpleName());

    public DeleteIndexServerRequest(ZuliaIndexManager indexManager) {
        super(indexManager);
    }

    @Override
    protected DeleteIndexResponse handleCall(ZuliaIndexManager indexManager, DeleteIndexRequest request) throws Exception {
        return indexManager.deleteIndex(request);
    }

    @Override
    protected void onError(Throwable e) {
        LOG.log(Level.SEVERE, "Failed to handle delete index", e);
    }
}
