package io.zulia.client.result;

import static io.zulia.message.ZuliaServiceOuterClass.DeleteIndexResponse;

public class DeleteIndexResult extends Result {

    @SuppressWarnings("unused")
    private DeleteIndexResponse deleteIndexResponse;

    public DeleteIndexResult(DeleteIndexResponse deleteIndexResponse) {
        this.deleteIndexResponse = deleteIndexResponse;
    }

}
