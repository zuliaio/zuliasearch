package io.zulia.client.result;

import static io.zulia.message.ZuliaServiceOuterClass.CreateIndexResponse;

public class CreateIndexResult extends Result {

	@SuppressWarnings("unused")
	private CreateIndexResponse indexCreateResponse;

	public CreateIndexResult(CreateIndexResponse indexCreateResponse) {
		this.indexCreateResponse = indexCreateResponse;
	}

}
