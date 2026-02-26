package io.zulia.client.result;

import io.zulia.client.config.ClientIndexConfig;
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

	public boolean isChanged() {
		return updateIndexResponse.getChanged();
	}

	public ClientIndexConfig getClientIndexConfig() {
		ClientIndexConfig clientIndexConfig = new ClientIndexConfig();
		clientIndexConfig.configure(updateIndexResponse.getFullIndexSettings());
		return clientIndexConfig;
	}
}
