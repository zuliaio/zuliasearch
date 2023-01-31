package io.zulia.client.command;

import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.command.base.SingleIndexRoutableCommand;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.GetNumberOfDocsResult;

import static io.zulia.message.ZuliaServiceGrpc.ZuliaServiceBlockingStub;
import static io.zulia.message.ZuliaServiceOuterClass.GetNumberOfDocsRequest;
import static io.zulia.message.ZuliaServiceOuterClass.GetNumberOfDocsResponse;

public class GetNumberOfDocs extends SimpleCommand<GetNumberOfDocsRequest, GetNumberOfDocsResult> implements SingleIndexRoutableCommand {

    private String indexName;

    public GetNumberOfDocs(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public String getIndexName() {
        return indexName;
    }

    @Override
    public GetNumberOfDocsRequest getRequest() {
        return GetNumberOfDocsRequest.newBuilder().setIndexName(indexName).build();
    }

    @Override
    public GetNumberOfDocsResult execute(ZuliaConnection zuliaConnection) {
        ZuliaServiceBlockingStub service = zuliaConnection.getService();

        GetNumberOfDocsResponse getNumberOfDocsResponse = service.getNumberOfDocs(getRequest());

        return new GetNumberOfDocsResult(getNumberOfDocsResponse);
    }

}
