package io.zulia.client.command;

import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.GetIndexesResult;
import io.zulia.message.ZuliaServiceGrpc;
import io.zulia.message.ZuliaServiceOuterClass;

import static io.zulia.message.ZuliaServiceOuterClass.GetIndexesRequest;
import static io.zulia.message.ZuliaServiceOuterClass.GetIndexesResponse;

public class GetIndexes extends SimpleCommand<ZuliaServiceOuterClass.GetIndexesRequest, GetIndexesResult> {

    public GetIndexes() {

    }

    @Override
    public GetIndexesRequest getRequest() {
        return GetIndexesRequest.newBuilder().build();
    }

    @Override
    public GetIndexesResult execute(ZuliaConnection zuliaConnection) {
        ZuliaServiceGrpc.ZuliaServiceBlockingStub service = zuliaConnection.getService();

        GetIndexesResponse getIndexesResponse = service.getIndexes(getRequest());

        return new GetIndexesResult(getIndexesResponse);
    }

}
