package io.zulia.client.result;

import io.zulia.message.ZuliaServiceOuterClass.CreateIndexAliasResponse;

public class CreateIndexAliasResult extends Result {

    @SuppressWarnings("unused")
    private CreateIndexAliasResponse createIndexAliasResponse;

    public CreateIndexAliasResult(CreateIndexAliasResponse createIndexAliasResponse) {
        this.createIndexAliasResponse = createIndexAliasResponse;
    }

}
