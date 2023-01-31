package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.DeleteIndexAliasRequest;
import io.zulia.message.ZuliaServiceOuterClass.DeleteIndexAliasResponse;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class DeleteIndexAliasServerRequest extends ServerRequestHandler<DeleteIndexAliasResponse, DeleteIndexAliasRequest> {

    private final static Logger LOG = Logger.getLogger(DeleteIndexAliasServerRequest.class.getSimpleName());

    public DeleteIndexAliasServerRequest(ZuliaIndexManager indexManager) {
        super(indexManager);
    }

    @Override
    protected DeleteIndexAliasResponse handleCall(ZuliaIndexManager indexManager, DeleteIndexAliasRequest request) throws Exception {
        return indexManager.deleteIndexAlias(request);
    }

    @Override
    protected void onError(Throwable e) {
        LOG.log(Level.SEVERE, "Failed to handle delete index alias", e);
    }
}
