package io.zulia.client.result;

import io.zulia.message.ZuliaServiceOuterClass;

public class DeleteResult extends Result {

    @SuppressWarnings("unused")
    private ZuliaServiceOuterClass.DeleteResponse deleteResponse;

    public DeleteResult(ZuliaServiceOuterClass.DeleteResponse deleteResponse) {
        this.deleteResponse = deleteResponse;
    }

}
