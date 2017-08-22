package io.zulia.server.index;

import io.zulia.message.ZuliaBase.Node;
import io.zulia.server.node.ZuliaNode;

import java.util.Collection;

public abstract class NodeRequestBase<I, O> {

	protected final Node thisNode;
	protected final Collection<Node> otherNodesActive;

	public NodeRequestBase(Node thisNode, Collection<Node> otherNodesActive) {

		this.thisNode = thisNode;
		this.otherNodesActive = otherNodesActive;

	}

	public boolean nodeIsLocal(Node node) {
		return ZuliaNode.isEqual(node, thisNode);
	}

	protected abstract O processExternal(Node node, I request) throws Exception;

	protected abstract O processInternal(I request) throws Exception;

}
