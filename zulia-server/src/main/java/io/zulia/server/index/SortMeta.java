package io.zulia.server.index;

import io.zulia.message.ZuliaIndex.FieldConfig;

public record SortMeta(String sortField, FieldConfig.FieldType sortFieldType) {
}
