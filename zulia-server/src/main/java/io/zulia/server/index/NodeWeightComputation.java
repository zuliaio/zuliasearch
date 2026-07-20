package io.zulia.server.index;

import io.zulia.message.ZuliaIndex;
import io.zulia.util.NodeKey;

import java.util.List;

public interface NodeWeightComputation {
	List<NodeKey> getNodesSortedByWeight();

	void addShard(NodeKey nodeKey, ZuliaIndex.IndexSettings indexSettings, boolean primary);
}
