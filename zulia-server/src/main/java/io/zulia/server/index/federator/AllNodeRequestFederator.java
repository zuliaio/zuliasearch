package io.zulia.server.index.federator;

import io.zulia.message.ZuliaBase.Node;

import java.util.Collection;
import java.util.concurrent.ExecutorService;

public abstract class AllNodeRequestFederator<I, O> extends NodeRequestFederator<I, O> {

	public AllNodeRequestFederator(Node thisNode, Collection<Node> otherNodesActive, ExecutorService pool) {
		super(thisNode, otherNodesActive, pool);

		nodes.add(thisNode);
		nodes.addAll(otherNodesActive);
	}

}
