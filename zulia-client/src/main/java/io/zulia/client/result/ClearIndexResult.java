package io.zulia.client.result;

import io.zulia.message.ZuliaServiceOuterClass.ClearResponse;

public class ClearIndexResult extends Result {

    @SuppressWarnings("unused")
    private ClearResponse clearResponse;

    public ClearIndexResult(ClearResponse clearResponse) {
        this.clearResponse = clearResponse;

    }

}
