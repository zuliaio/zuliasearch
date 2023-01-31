package io.zulia.server.index.federator;

import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaServiceOuterClass.DeleteIndexRequest;
import io.zulia.message.ZuliaServiceOuterClass.DeleteIndexResponse;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.Collection;
import java.util.concurrent.ExecutorService;

public class DeleteIndexRequestFederator extends AllNodeRequestFederator<DeleteIndexRequest, DeleteIndexResponse> {
    private final InternalClient internalClient;
    private final ZuliaIndexManager indexManager;

    public DeleteIndexRequestFederator(Node thisNode, Collection<Node> otherNodesActive, ExecutorService pool, InternalClient internalClient,
                                       ZuliaIndexManager indexManager) {
        super(thisNode, otherNodesActive, pool);

        this.internalClient = internalClient;
        this.indexManager = indexManager;
    }

    @Override
    protected DeleteIndexResponse processExternal(Node node, DeleteIndexRequest request) throws Exception {
        return internalClient.deleteIndex(node, request);
    }

    @Override
    protected DeleteIndexResponse processInternal(Node node, DeleteIndexRequest request) throws Exception {
        return internalDeleteIndex(indexManager, request);
    }

    public static DeleteIndexResponse internalDeleteIndex(ZuliaIndexManager zuliaIndexManager, DeleteIndexRequest request) throws Exception {
        return zuliaIndexManager.internalDeleteIndex(request);
    }

}
