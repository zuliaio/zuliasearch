package io.zulia.client.result;

import static io.zulia.message.ZuliaServiceOuterClass.CreateIndexResponse;

public class CreateIndexResult extends Result {

	@SuppressWarnings("unused")
	private final CreateIndexResponse indexCreateResponse;

	public CreateIndexResult(CreateIndexResponse indexCreateResponse) {
		this.indexCreateResponse = indexCreateResponse;
	}

	public boolean isChanged() {
		return indexCreateResponse.getChanged();
	}

}
