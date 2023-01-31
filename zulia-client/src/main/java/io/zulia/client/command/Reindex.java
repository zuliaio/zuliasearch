package io.zulia.client.command;

import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.command.base.SingleIndexRoutableCommand;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.ReindexResult;
import io.zulia.message.ZuliaServiceOuterClass.ReindexRequest;
import io.zulia.message.ZuliaServiceOuterClass.ReindexResponse;

import static io.zulia.message.ZuliaServiceGrpc.ZuliaServiceBlockingStub;

/**
 * Reindexes a given index
 *
 * @author mdavis
 */
public class Reindex extends SimpleCommand<ReindexRequest, ReindexResult> implements SingleIndexRoutableCommand {

	private String indexName;

	public Reindex(String indexName) {
		this.indexName = indexName;
	}

	@Override
	public String getIndexName() {
		return indexName;
	}

	@Override
	public ReindexRequest getRequest() {
		return ReindexRequest.newBuilder().setIndexName(indexName).build();
	}

	@Override
	public ReindexResult execute(ZuliaConnection zuliaConnection) {
		ZuliaServiceBlockingStub service = zuliaConnection.getService();

		ReindexResponse reindexResponse = service.reindex(getRequest());

		return new ReindexResult(reindexResponse);
	}

}
