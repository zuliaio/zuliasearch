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
 *
 * @author mdavis
 */
public class OptimizeIndex extends SimpleCommand<ZuliaServiceOuterClass.OptimizeRequest, OptimizeIndexResult> implements SingleIndexRoutableCommand {

	private String indexName;
	private int maxNumberOfSegments;

	public OptimizeIndex(String indexName) {
		this.indexName = indexName;
	}

	@Override
	public String getIndexName() {
		return indexName;
	}

	/**
	 * Merges each primary shard down to at most this many segments. 0, the default, merges each shard to one segment.
	 */
	public OptimizeIndex setMaxNumberOfSegments(int maxNumberOfSegments) {
		if (maxNumberOfSegments < 0) {
			throw new IllegalArgumentException(
					"maxNumberOfSegments for optimize of index <" + indexName + "> must be 0 (one segment) or a positive segment count, got "
							+ maxNumberOfSegments);
		}
		this.maxNumberOfSegments = maxNumberOfSegments;
		return this;
	}

	public int getMaxNumberOfSegments() {
		return maxNumberOfSegments;
	}

	@Override
	public OptimizeRequest getRequest() {
		return OptimizeRequest.newBuilder().setIndexName(indexName).setMaxNumberOfSegments(maxNumberOfSegments).build();
	}

	@Override
	public OptimizeIndexResult execute(ZuliaConnection zuliaConnection) {
		ZuliaServiceBlockingStub service = zuliaConnection.getService();

		OptimizeResponse optimizeResponse = service.optimize(getRequest());

		return new OptimizeIndexResult(optimizeResponse);
	}

}
