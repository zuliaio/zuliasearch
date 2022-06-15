package io.zulia.server.index.federator;

import io.zulia.message.ZuliaBase.Node;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.Collection;
import java.util.concurrent.ExecutorService;

import static io.zulia.message.ZuliaServiceOuterClass.InternalCreateOrUpdateIndexRequest;
import static io.zulia.message.ZuliaServiceOuterClass.InternalCreateOrUpdateIndexResponse;

public class CreateOrUpdateIndexRequestFederator extends AllNodeRequestFederator<InternalCreateOrUpdateIndexRequest, InternalCreateOrUpdateIndexResponse> {
	private final InternalClient internalClient;
	private final ZuliaIndexManager indexManager;

	public CreateOrUpdateIndexRequestFederator(Node thisNode, Collection<Node> otherNodesActive, ExecutorService pool, InternalClient internalClient,
			ZuliaIndexManager indexManager) {
		super(thisNode, otherNodesActive, pool);

		this.internalClient = internalClient;
		this.indexManager = indexManager;
	}

	@Override
	protected InternalCreateOrUpdateIndexResponse processExternal(Node node, InternalCreateOrUpdateIndexRequest request) throws Exception {
		return internalClient.createOrUpdateIndex(node, request);
	}

	@Override
	protected InternalCreateOrUpdateIndexResponse processInternal(Node node, InternalCreateOrUpdateIndexRequest request) throws Exception {
		return internalCreateOrUpdateIndex(indexManager, request);
	}

	public static InternalCreateOrUpdateIndexResponse internalCreateOrUpdateIndex(ZuliaIndexManager zuliaIndexManager,
			InternalCreateOrUpdateIndexRequest request) throws Exception {
		return zuliaIndexManager.internalCreateOrUpdateIndex(request.getIndexName());
	}

}
