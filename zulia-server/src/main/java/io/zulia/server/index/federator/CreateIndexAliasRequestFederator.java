package io.zulia.server.index.federator;

import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaServiceOuterClass.CreateIndexAliasResponse;
import io.zulia.message.ZuliaServiceOuterClass.InternalCreateIndexAliasRequest;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.Collection;
import java.util.concurrent.ExecutorService;

public class CreateIndexAliasRequestFederator extends AllNodeRequestFederator<InternalCreateIndexAliasRequest, CreateIndexAliasResponse> {
    private final InternalClient internalClient;
    private final ZuliaIndexManager indexManager;

    public CreateIndexAliasRequestFederator(Node thisNode, Collection<Node> otherNodesActive, ExecutorService pool, InternalClient internalClient,
                                            ZuliaIndexManager indexManager) {
        super(thisNode, otherNodesActive, pool);

        this.internalClient = internalClient;
        this.indexManager = indexManager;
    }

    public static CreateIndexAliasResponse internalCreateIndexAlias(ZuliaIndexManager zuliaIndexManager, InternalCreateIndexAliasRequest request)
            throws Exception {
        return zuliaIndexManager.internalCreateIndexAlias(request.getAliasName());
    }

    @Override
    protected CreateIndexAliasResponse processExternal(Node node, InternalCreateIndexAliasRequest request) throws Exception {
        return internalClient.createIndexAlias(node, request);
    }

    @Override
    protected CreateIndexAliasResponse processInternal(Node node, InternalCreateIndexAliasRequest request) throws Exception {
        return internalCreateIndexAlias(indexManager, request);
    }
}
