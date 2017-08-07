package io.zulia.client.command;

import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.DeleteIndexResult;

import static io.zulia.message.ZuliaServiceGrpc.ZuliaServiceBlockingStub;
import static io.zulia.message.ZuliaServiceOuterClass.DeleteIndexRequest;
import static io.zulia.message.ZuliaServiceOuterClass.DeleteIndexResponse;

/**
 * Deletes an index.  If index does not exist throwns an exception
 * @author mdavis
 *
 */
public class DeleteIndex extends SimpleCommand<DeleteIndexRequest, DeleteIndexResult> {

	private String indexName;

	public DeleteIndex(String indexName) {
		this.indexName = indexName;
	}

	@Override
	public DeleteIndexRequest getRequest() {
		return DeleteIndexRequest.newBuilder().setIndexName(indexName).build();
	}

	@Override
	public DeleteIndexResult execute(ZuliaConnection zuliaConnection) {
		ZuliaServiceBlockingStub service = zuliaConnection.getService();

		DeleteIndexResponse indexDeleteResponse = service.deleteIndex(getRequest());

		return new DeleteIndexResult(indexDeleteResponse);
	}

}
