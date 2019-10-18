package io.zulia.client.result;

import io.zulia.message.ZuliaServiceOuterClass.ReindexResponse;

public class ReindexResult extends Result {

	@SuppressWarnings("unused")
	private ReindexResponse reindexResponse;

	public ReindexResult(ReindexResponse reindexResponse) {
		this.reindexResponse = reindexResponse;
	}

}
