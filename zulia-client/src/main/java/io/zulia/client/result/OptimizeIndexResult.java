package io.zulia.client.result;

import static io.zulia.message.ZuliaServiceOuterClass.OptimizeResponse;

public class OptimizeIndexResult extends Result {

    @SuppressWarnings("unused")
    private OptimizeResponse optimizeResponse;

    public OptimizeIndexResult(OptimizeResponse optimizeResponse) {
        this.optimizeResponse = optimizeResponse;
    }

}
