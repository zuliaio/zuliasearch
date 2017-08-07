package io.zulia.client.command;

import io.zulia.message.ZuliaQuery.FetchType;

public class FetchDocumentAndAssociated extends Fetch {

	public FetchDocumentAndAssociated(String uniqueId, String indexName) {
		this(uniqueId, indexName, false);
	}

	public FetchDocumentAndAssociated(String uniqueId, String indexName, boolean metaOnly) {
		super(uniqueId, indexName);
		setResultFetchType(FetchType.FULL);
		if (metaOnly) {
			setAssociatedFetchType(FetchType.META);
		}
		else {
			setAssociatedFetchType(FetchType.FULL);
		}
	}

}
