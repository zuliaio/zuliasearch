package io.zulia.client.command.builder;

import io.zulia.message.ZuliaQuery.FieldSort;

public interface SortBuilder {

    FieldSort getSort();
}
