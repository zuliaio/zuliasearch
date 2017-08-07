package io.zulia.client.command;

import io.zulia.message.ZuliaQuery.FetchType;

public class FetchAllAssociated extends Fetch {

	public FetchAllAssociated(String uniqueId, String indexName) {
		super(uniqueId, indexName);
		setResultFetchType(FetchType.NONE);
		setAssociatedFetchType(FetchType.FULL);
	}

}
