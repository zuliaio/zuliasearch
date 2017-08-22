package io.zulia.server.index.federator;

import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaServiceOuterClass.ClearRequest;
import io.zulia.message.ZuliaServiceOuterClass.ClearResponse;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.index.ZuliaIndex;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

public class ClearRequestFederator extends MasterSlaveNodeRequestFederator<ClearRequest, ClearResponse> {
	private final InternalClient internalClient;
	private final ZuliaIndex index;

	public ClearRequestFederator(Node thisNode, Collection<Node> otherNodesActive, ZuliaBase.MasterSlaveSettings masterSlaveSettings, ZuliaIndex index,
			ExecutorService pool, InternalClient internalClient) throws IOException {
		super(thisNode, otherNodesActive, masterSlaveSettings, index, pool);
		this.internalClient = internalClient;
		this.index = index;
	}

	@Override
	protected ClearResponse processExternal(Node node, ClearRequest request) throws Exception {
		return internalClient.clear(node, request);
	}

	@Override
	protected ClearResponse processInternal(ClearRequest request) throws Exception {
		return internalClear(index, request);
	}

	public static ClearResponse internalClear(ZuliaIndex index, ClearRequest request) throws Exception {
		return index.clear(request);
	}

	public ClearResponse getResponse(ClearRequest request) throws Exception {
		send(request);
		return ClearResponse.newBuilder().build();
	}
}
