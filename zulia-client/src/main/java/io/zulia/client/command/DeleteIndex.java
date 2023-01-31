package io.zulia.client.command;

import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.command.base.SingleIndexRoutableCommand;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.DeleteIndexResult;

import static io.zulia.message.ZuliaServiceGrpc.ZuliaServiceBlockingStub;
import static io.zulia.message.ZuliaServiceOuterClass.DeleteIndexRequest;
import static io.zulia.message.ZuliaServiceOuterClass.DeleteIndexResponse;

/**
 * Deletes an index.  If index does not exist throws an exception
 *
 * @author mdavis
 */
public class DeleteIndex extends SimpleCommand<DeleteIndexRequest, DeleteIndexResult> implements SingleIndexRoutableCommand {

    private String indexName;
    private boolean deleteAssociated;

    public DeleteIndex(String indexName) {
        this.indexName = indexName;
    }

    public DeleteIndex setIndexName(String indexName) {
        this.indexName = indexName;
        return this;
    }

    public boolean isDeleteAssociated() {
        return deleteAssociated;
    }

    public DeleteIndex setDeleteAssociated(boolean deleteAssociated) {
        this.deleteAssociated = deleteAssociated;
        return this;
    }

    @Override
    public String getIndexName() {
        return indexName;
    }

    @Override
    public DeleteIndexRequest getRequest() {
        return DeleteIndexRequest.newBuilder().setIndexName(indexName).setDeleteAssociated(deleteAssociated).build();
    }

    @Override
    public DeleteIndexResult execute(ZuliaConnection zuliaConnection) {
        ZuliaServiceBlockingStub service = zuliaConnection.getService();

        DeleteIndexResponse indexDeleteResponse = service.deleteIndex(getRequest());

        return new DeleteIndexResult(indexDeleteResponse);
    }

}
