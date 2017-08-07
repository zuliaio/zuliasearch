package io.zulia.client.command;

import static io.zulia.message.ZuliaQuery.FetchType;
import static io.zulia.message.ZuliaQuery.ScoredResult;

public class FetchDocument extends Fetch {

	public FetchDocument(ScoredResult sr) {
		this(sr.getUniqueId(), sr.getIndexName());
	}

	public FetchDocument(String uniqueId, String indexName) {
		super(uniqueId, indexName);
		setResultFetchType(FetchType.FULL);
		setAssociatedFetchType(FetchType.NONE);
	}

}
