package io.zulia.client.command;

import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.command.base.SingleIndexRoutableCommand;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.ClearIndexResult;
import io.zulia.message.ZuliaServiceGrpc;
import io.zulia.message.ZuliaServiceOuterClass;
import io.zulia.message.ZuliaServiceOuterClass.ClearRequest;

/**
 * Removes all documents from a given index
 *
 * @author mdavis
 */
public class ClearIndex extends SimpleCommand<ClearRequest, ClearIndexResult> implements SingleIndexRoutableCommand {

    private String indexName;

    public ClearIndex(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public String getIndexName() {
        return indexName;
    }

    @Override
    public ClearRequest getRequest() {
        return ClearRequest.newBuilder().setIndexName(indexName).build();
    }

    @Override
    public ClearIndexResult execute(ZuliaConnection zuliaConnection) {
        ZuliaServiceGrpc.ZuliaServiceBlockingStub service = zuliaConnection.getService();

        ZuliaServiceOuterClass.ClearResponse clearResponse = service.clear(getRequest());

        return new ClearIndexResult(clearResponse);
    }

}
