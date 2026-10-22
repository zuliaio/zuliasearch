package io.zulia.client.command.builder;

import io.zulia.message.ZuliaQuery.DistinctCountRequest;

public interface DistinctCountBuilder {

	DistinctCountRequest getDistinctCountRequest();
}
