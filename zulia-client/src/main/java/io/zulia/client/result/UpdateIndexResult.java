package io.zulia.client.result;

import io.zulia.message.ZuliaServiceOuterClass.UpdateIndexResponse;

import static io.zulia.message.ZuliaServiceOuterClass.CreateIndexResponse;

public class UpdateIndexResult extends Result {

	@SuppressWarnings("unused")
	private UpdateIndexResponse updateIndexResponse;

	public UpdateIndexResult(UpdateIndexResponse updateIndexResponse) {
		this.updateIndexResponse = updateIndexResponse;
	}

}
