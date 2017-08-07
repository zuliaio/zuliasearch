package io.zulia.client.result;

import org.lumongo.cluster.message.Lumongo.IndexSettingsResponse;

public class UpdateIndexResult extends Result {

	@SuppressWarnings("unused")
	private IndexSettingsResponse indexSettingsResponse;

	public UpdateIndexResult(IndexSettingsResponse indexSettingsResponse) {
		this.indexSettingsResponse = indexSettingsResponse;
	}

}
