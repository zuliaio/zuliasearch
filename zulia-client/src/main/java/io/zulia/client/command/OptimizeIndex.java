package io.zulia.client.command;

import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.command.base.SingleIndexRoutableCommand;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.OptimizeIndexResult;
import io.zulia.message.ZuliaServiceOuterClass;

import static io.zulia.message.ZuliaServiceGrpc.ZuliaServiceBlockingStub;
import static io.zulia.message.ZuliaServiceOuterClass.OptimizeRequest;
import static io.zulia.message.ZuliaServiceOuterClass.OptimizeResponse;

/**
 * Optimizes a given index
 * @author mdavis
 *
 */
public class OptimizeIndex extends SimpleCommand<ZuliaServiceOuterClass.OptimizeRequest, OptimizeIndexResult> implements SingleIndexRoutableCommand {

	private String indexName;

	public OptimizeIndex(String indexName) {
		this.indexName = indexName;
	}

	@Override
	public String getIndexName() {
		return indexName;
	}

	@Override
	public OptimizeRequest getRequest() {
		return OptimizeRequest.newBuilder().setIndexName(indexName).build();
	}

	@Override
	public OptimizeIndexResult execute(ZuliaConnection zuliaConnection) {
		ZuliaServiceBlockingStub service = zuliaConnection.getService();

		OptimizeResponse optimizeResponse = service.optimize(getRequest());

		return new OptimizeIndexResult(optimizeResponse);
	}

}
