package io.zulia.client.command;

import io.zulia.message.ZuliaQuery.FetchType;

public class FetchAssociated extends Fetch {

    public FetchAssociated(String uniqueId, String indexName, String fileName) {
        super(uniqueId, indexName);
        setFilename(fileName);
        setResultFetchType(FetchType.NONE);
        setAssociatedFetchType(FetchType.FULL);
    }

}
