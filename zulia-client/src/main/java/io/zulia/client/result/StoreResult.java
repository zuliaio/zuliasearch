package io.zulia.client.result;

import static io.zulia.message.ZuliaServiceOuterClass.StoreResponse;

public class StoreResult extends Result {

	@SuppressWarnings("unused")
	private StoreResponse storeResponse;

	public StoreResult(StoreResponse storeResponse) {
		this.storeResponse = storeResponse;
	}

}
