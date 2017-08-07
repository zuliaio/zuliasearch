package io.zulia.client.result;

import io.zulia.message.ZuliaServiceOuterClass.BatchDeleteResponse;

public class BatchDeleteResult extends Result {

	@SuppressWarnings("unused")
	private BatchDeleteResponse batchDeleteResponse;

	public BatchDeleteResult(BatchDeleteResponse batchDeleteResponse) {
		this.batchDeleteResponse = batchDeleteResponse;
	}

}
