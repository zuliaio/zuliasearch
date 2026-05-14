package io.zulia.server.index.router;

import io.zulia.message.ZuliaBase.PrimaryReplicaSettings;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.server.index.PrimaryReplicaSelector;
import io.zulia.server.index.NodeRequestBase;
import io.zulia.server.index.ZuliaIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class NodeRequestRouter<I, O> extends NodeRequestBase<I, O> {

	private Node node;

	public NodeRequestRouter(Node thisNode, Collection<Node> otherNodesActive, PrimaryReplicaSettings primaryReplicaSettings, ZuliaIndex index, String uniqueId)
			throws IOException {

		super(thisNode, otherNodesActive);

		List<Node> nodeList = new ArrayList<>();
		nodeList.add(thisNode);
		nodeList.addAll(otherNodesActive);

		this.node = new PrimaryReplicaSelector(primaryReplicaSettings, nodeList, index.getIndexShardMapping()).getNodeForUniqueId(uniqueId);

	}

	public O send(final I request) throws Exception {
		if (nodeIsLocal(node)) {
			return processInternal(node, request);
		}
		return processExternal(node, request);
	}

}
