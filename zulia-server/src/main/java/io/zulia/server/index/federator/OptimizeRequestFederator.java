package io.zulia.server.index.federator;

import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaServiceOuterClass.OptimizeRequest;
import io.zulia.message.ZuliaServiceOuterClass.OptimizeResponse;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.index.ZuliaIndex;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

public class OptimizeRequestFederator extends MasterSlaveNodeRequestFederator<OptimizeRequest, OptimizeResponse> {
	private final InternalClient internalClient;
	private final ZuliaIndex index;

	public OptimizeRequestFederator(Node thisNode, Collection<Node> otherNodesActive, ZuliaBase.MasterSlaveSettings masterSlaveSettings, ZuliaIndex index,
			ExecutorService pool, InternalClient internalClient) throws IOException {
		super(thisNode, otherNodesActive, masterSlaveSettings, index, pool);
		this.internalClient = internalClient;
		this.index = index;
	}

	@Override
	protected OptimizeResponse processExternal(Node node, OptimizeRequest request) throws Exception {
		return internalClient.optimize(node, request);
	}

	@Override
	protected OptimizeResponse processInternal(OptimizeRequest request) throws Exception {
		return internalOptimize(index, request);
	}

	public static OptimizeResponse internalOptimize(ZuliaIndex index, OptimizeRequest request) throws Exception {
		return index.optimize(request);
	}

	public OptimizeResponse getResponse(OptimizeRequest request) throws Exception {
		send(request);
		return OptimizeResponse.newBuilder().build();
	}
}
