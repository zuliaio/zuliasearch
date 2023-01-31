package io.zulia.client.command.builder;

import io.zulia.message.ZuliaQuery;

public interface StatBuilder {

    ZuliaQuery.StatRequest getStatRequest();
}
