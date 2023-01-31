package io.zulia.client.result;

import io.zulia.message.ZuliaServiceOuterClass.DeleteResponse;

import java.util.Iterator;

public class BatchDeleteResult extends Result {

    @SuppressWarnings("unused")
    private Iterator<DeleteResponse> batchDeleteResponse;

    public BatchDeleteResult(Iterator<DeleteResponse> batchDeleteResponse) {
        this.batchDeleteResponse = batchDeleteResponse;
    }

}
