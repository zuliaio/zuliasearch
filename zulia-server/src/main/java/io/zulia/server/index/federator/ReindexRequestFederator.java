package io.zulia.server.index.federator;

import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaServiceOuterClass.ReindexRequest;
import io.zulia.message.ZuliaServiceOuterClass.ReindexResponse;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.index.ZuliaIndex;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

public class ReindexRequestFederator extends MasterSlaveNodeRequestFederator<ReindexRequest, ReindexResponse> {
	private final InternalClient internalClient;
	private final ZuliaIndex index;

	public ReindexRequestFederator(Node thisNode, Collection<Node> otherNodesActive, ZuliaBase.MasterSlaveSettings masterSlaveSettings, ZuliaIndex index,
			ExecutorService pool, InternalClient internalClient) throws IOException {
		super(thisNode, otherNodesActive, masterSlaveSettings, index, pool);
		this.internalClient = internalClient;
		this.index = index;
	}

	@Override
	protected ReindexResponse processExternal(Node node, ReindexRequest request) throws Exception {
		return internalClient.reindex(node, request);
	}

	@Override
	protected ReindexResponse processInternal(Node node, ReindexRequest request) throws Exception {
		return internalReindex(index, request);
	}

	public static ReindexResponse internalReindex(ZuliaIndex index, ReindexRequest request) throws Exception {
		return index.reindex(request);
	}

	public ReindexResponse getResponse(ReindexRequest request) throws Exception {
		send(request);
		return ReindexResponse.newBuilder().build();
	}
}
