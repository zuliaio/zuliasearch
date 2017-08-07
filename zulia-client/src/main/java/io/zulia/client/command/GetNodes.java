package io.zulia.client.command;

import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.GetNodesResult;

import static io.zulia.message.ZuliaServiceGrpc.ZuliaServiceBlockingStub;
import static io.zulia.message.ZuliaServiceOuterClass.GetNodesRequest;
import static io.zulia.message.ZuliaServiceOuterClass.GetNodesResponse;

/**
 * Returns the current cluster members list
 * @author mdavis
 *
 */
public class GetNodes extends SimpleCommand<GetNodesRequest, GetNodesResult> {

	public GetNodes() {

	}

	@Override
	public GetNodesRequest getRequest() {
		return GetNodesRequest.newBuilder().build();
	}

	@Override
	public GetNodesResult execute(ZuliaConnection zuliaConnection) {
		ZuliaServiceBlockingStub service = zuliaConnection.getService();

		GetNodesResponse getMembersResponse = service.getNodes(getRequest());

		return new GetNodesResult(getMembersResponse);
	}

}
