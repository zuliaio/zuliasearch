package io.zulia.server.index.federator;

import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaServiceOuterClass.DeleteIndexAliasRequest;
import io.zulia.message.ZuliaServiceOuterClass.DeleteIndexAliasResponse;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.Collection;
import java.util.concurrent.ExecutorService;

public class DeleteIndexAliasRequestFederator extends AllNodeRequestFederator<DeleteIndexAliasRequest, DeleteIndexAliasResponse> {
	private final InternalClient internalClient;
	private final ZuliaIndexManager indexManager;

	public DeleteIndexAliasRequestFederator(Node thisNode, Collection<Node> otherNodesActive, ExecutorService pool, InternalClient internalClient,
			ZuliaIndexManager indexManager) {
		super(thisNode, otherNodesActive, pool);

		this.internalClient = internalClient;
		this.indexManager = indexManager;
	}

	@Override
	protected DeleteIndexAliasResponse processExternal(Node node, DeleteIndexAliasRequest request) throws Exception {
		return internalClient.deleteIndexAlias(node, request);
	}

	@Override
	protected DeleteIndexAliasResponse processInternal(Node node, DeleteIndexAliasRequest request) throws Exception {
		return internalDeleteIndexAlias(indexManager, request);
	}

	public static DeleteIndexAliasResponse internalDeleteIndexAlias(ZuliaIndexManager zuliaIndexManager, DeleteIndexAliasRequest request) throws Exception {
		return zuliaIndexManager.internalDeleteIndexAlias(request);
	}

}
