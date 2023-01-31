package io.zulia.server.index;

import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaIndex;

import java.util.List;

public interface NodeWeightComputation {
	List<ZuliaBase.Node> getNodesSortedByWeight();

	void addShard(ZuliaBase.Node node, ZuliaIndex.IndexSettings indexSettings, boolean primary);
}
