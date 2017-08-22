package io.zulia.server.index.federator;

import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.server.exceptions.ShardOfflineException;
import io.zulia.server.index.MasterSlaveSelector;
import io.zulia.server.index.ZuliaIndex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

public abstract class MasterSlaveNodeRequestFederator<I, O> extends NodeRequestFederator<I, O> {

	public MasterSlaveNodeRequestFederator(Node thisNode, Collection<Node> otherNodesActive, ZuliaBase.MasterSlaveSettings masterSlaveSettings,
			ZuliaIndex index, ExecutorService pool) throws ShardOfflineException {
		this(thisNode, otherNodesActive, masterSlaveSettings, Collections.singletonList(index), pool);
	}

	public MasterSlaveNodeRequestFederator(Node thisNode, Collection<Node> otherNodesActive, ZuliaBase.MasterSlaveSettings masterSlaveSettings,
			Collection<ZuliaIndex> indexes, ExecutorService pool) throws ShardOfflineException {
		super(thisNode, otherNodesActive, pool);

		List<Node> nodesAvailable = new ArrayList<>();
		nodesAvailable.add(thisNode);
		nodesAvailable.addAll(otherNodesActive);

		for (ZuliaIndex index : indexes) {
			io.zulia.message.ZuliaIndex.IndexMapping indexMapping = index.getIndexMapping();
			new MasterSlaveSelector(masterSlaveSettings, nodesAvailable, indexMapping).addNodesForIndex(nodes);
		}
	}
}
