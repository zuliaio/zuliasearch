package io.zulia.server.index.federator;

import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaServiceOuterClass.CreateIndexResponse;
import io.zulia.message.ZuliaServiceOuterClass.InternalCreateIndexRequest;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.Collection;
import java.util.concurrent.ExecutorService;

public class CreateIndexRequestFederator extends AllNodeRequestFederator<InternalCreateIndexRequest, CreateIndexResponse> {
	private final InternalClient internalClient;
	private final ZuliaIndexManager indexManager;

	public CreateIndexRequestFederator(Node thisNode, Collection<Node> otherNodesActive, ExecutorService pool, InternalClient internalClient,
			ZuliaIndexManager indexManager) {
		super(thisNode, otherNodesActive, pool);

		this.internalClient = internalClient;
		this.indexManager = indexManager;
	}

	@Override
	protected CreateIndexResponse processExternal(Node node, InternalCreateIndexRequest request) throws Exception {
		return internalClient.createIndex(node, request);
	}

	@Override
	protected CreateIndexResponse processInternal(Node node, InternalCreateIndexRequest request) throws Exception {
		return internalCreateIndex(indexManager, request);
	}

	public static CreateIndexResponse internalCreateIndex(ZuliaIndexManager zuliaIndexManager, InternalCreateIndexRequest request) throws Exception {
		return zuliaIndexManager.internalCreateIndex(request.getIndexName());
	}

}
