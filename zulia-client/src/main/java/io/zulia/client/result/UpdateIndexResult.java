package io.zulia.client.result;

import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaServiceOuterClass.UpdateIndexResponse;

public class UpdateIndexResult extends Result {

	@SuppressWarnings("unused")
	private final UpdateIndexResponse updateIndexResponse;

	public UpdateIndexResult(UpdateIndexResponse updateIndexResponse) {
		this.updateIndexResponse = updateIndexResponse;
	}

	public ZuliaIndex.IndexSettings getFullIndexSettings() {
		return updateIndexResponse.getFullIndexSettings();
	}
}
