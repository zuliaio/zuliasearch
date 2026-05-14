package io.zulia.server.index.federator;

import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaServiceOuterClass.IndexRouting;
import io.zulia.server.exceptions.ShardOfflineException;
import io.zulia.server.index.PrimaryReplicaSelector;
import io.zulia.server.index.ZuliaIndex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public abstract class PrimaryReplicaNodeRequestFederator<I, O> extends NodeRequestFederator<I, O> {

	private Map<Node, List<IndexRouting>> nodeToRouting = new HashMap<>();

	public PrimaryReplicaNodeRequestFederator(Node thisNode, Collection<Node> otherNodesActive, ZuliaBase.PrimaryReplicaSettings primaryReplicaSettings,
			ZuliaIndex index, ExecutorService pool) throws ShardOfflineException {
		this(thisNode, otherNodesActive, primaryReplicaSettings, Collections.singletonList(index), pool);
	}

	public PrimaryReplicaNodeRequestFederator(Node thisNode, Collection<Node> otherNodesActive, ZuliaBase.PrimaryReplicaSettings primaryReplicaSettings,
			Collection<ZuliaIndex> indexes, ExecutorService pool) throws ShardOfflineException {
		super(thisNode, otherNodesActive, pool);

		List<Node> nodesAvailable = new ArrayList<>();
		nodesAvailable.add(thisNode);
		nodesAvailable.addAll(otherNodesActive);

		for (ZuliaIndex index : indexes) {
			io.zulia.message.ZuliaIndex.IndexShardMapping indexShardMapping = index.getIndexShardMapping();
			PrimaryReplicaSelector primaryReplicaSelector = new PrimaryReplicaSelector(primaryReplicaSettings, nodesAvailable, indexShardMapping);

			Map<Node, IndexRouting.Builder> nodesForIndex = primaryReplicaSelector.getNodesForIndex();

			for (Node node : nodesForIndex.keySet()) {
				IndexRouting indexRouting = nodesForIndex.get(node).setIndex(index.getIndexName()).build();
				nodeToRouting.computeIfAbsent(node, (k) -> new ArrayList<>()).add(indexRouting);
				nodes.add(node);
			}

		}
	}

	public List<IndexRouting> getIndexRouting(Node node) {
		return nodeToRouting.get(node);
	}
}
