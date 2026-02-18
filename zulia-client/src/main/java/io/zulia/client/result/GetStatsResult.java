package io.zulia.client.result;

import io.zulia.message.ZuliaBase.NodeStats;

public class GetStatsResult extends Result {

	private final NodeStats nodeStats;

	public GetStatsResult(NodeStats nodeStats) {
		this.nodeStats = nodeStats;
	}

	public NodeStats getNodeStats() {
		return nodeStats;
	}

}
