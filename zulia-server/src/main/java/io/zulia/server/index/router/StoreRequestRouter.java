package io.zulia.server.index.router;

import io.zulia.message.ZuliaBase.MasterSlaveSettings;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaServiceOuterClass.StoreRequest;
import io.zulia.message.ZuliaServiceOuterClass.StoreResponse;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.index.ZuliaIndex;

import java.io.IOException;
import java.util.Collection;

public class StoreRequestRouter extends NodeRequestRouter<StoreRequest, StoreResponse> {
	private final InternalClient internalClient;
	private final ZuliaIndex index;

	public StoreRequestRouter(Node thisNode, Collection<Node> otherNodesActive, ZuliaIndex index, String uniqueId,
			InternalClient internalClient) throws IOException {
		super(thisNode, otherNodesActive, MasterSlaveSettings.MASTER_ONLY, index, uniqueId);
		this.internalClient = internalClient;
		this.index = index;
	}

	@Override
	protected StoreResponse processExternal(Node node, StoreRequest request) throws Exception {
		return internalClient.executeStore(node, request);
	}

	@Override
	protected StoreResponse processInternal(StoreRequest request) throws Exception {
		return internalStore(index, request);
	}

	public static StoreResponse internalStore(ZuliaIndex index, StoreRequest request) throws Exception {
		return index.internalStore(request);
	}
}

