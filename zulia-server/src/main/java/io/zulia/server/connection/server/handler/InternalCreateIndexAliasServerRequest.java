package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.CreateIndexAliasResponse;
import io.zulia.message.ZuliaServiceOuterClass.InternalCreateIndexAliasRequest;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class InternalCreateIndexAliasServerRequest extends ServerRequestHandler<CreateIndexAliasResponse, InternalCreateIndexAliasRequest> {

    private final static Logger LOG = Logger.getLogger(InternalCreateIndexAliasServerRequest.class.getSimpleName());

    public InternalCreateIndexAliasServerRequest(ZuliaIndexManager indexManager) {
        super(indexManager);
    }

    @Override
    protected CreateIndexAliasResponse handleCall(ZuliaIndexManager indexManager, InternalCreateIndexAliasRequest request) throws Exception {
        return indexManager.internalCreateIndexAlias(request.getAliasName());
    }

    @Override
    protected void onError(Throwable e) {
        LOG.log(Level.SEVERE, "Failed to handle internal create index alias", e);
    }
}
