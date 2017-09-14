package io.zulia.server.index.router;

import io.zulia.message.ZuliaBase.MasterSlaveSettings;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaServiceOuterClass.FetchRequest;
import io.zulia.message.ZuliaServiceOuterClass.FetchResponse;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.index.ZuliaIndex;

import java.io.IOException;
import java.util.Collection;

public class FetchRequestRouter extends NodeRequestRouter<FetchRequest, FetchResponse> {
	private final InternalClient internalClient;
	private final ZuliaIndex index;

	public FetchRequestRouter(Node thisNode, Collection<Node> otherNodesActive, MasterSlaveSettings masterSlaveSettings, ZuliaIndex index, String uniqueId,
			InternalClient internalClient) throws IOException {
		super(thisNode, otherNodesActive, masterSlaveSettings, index, uniqueId);
		this.internalClient = internalClient;
		this.index = index;
	}

	@Override
	protected FetchResponse processExternal(Node node, FetchRequest request) throws Exception {
		return internalClient.executeFetch(node, request);
	}

	@Override
	protected FetchResponse processInternal(Node node, FetchRequest request) throws Exception {
		return internalFetch(index, request);
	}

	public static FetchResponse internalFetch(ZuliaIndex index, FetchRequest request) throws Exception {
		return index.fetch(request);
	}
}

