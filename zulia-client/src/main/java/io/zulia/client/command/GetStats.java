package io.zulia.client.command;

import io.zulia.client.command.base.RESTCommand;
import io.zulia.client.rest.ZuliaRESTClient;
import io.zulia.client.result.GetStatsResult;
import io.zulia.message.ZuliaBase.NodeStats;

public class GetStats extends RESTCommand<GetStatsResult> {

	@Override
	public GetStatsResult execute(ZuliaRESTClient zuliaRESTClient) throws Exception {
		NodeStats nodeStats = zuliaRESTClient.getStats();
		return new GetStatsResult(nodeStats);
	}

}
