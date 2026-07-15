package io.zulia.server.index.resident;

import io.zulia.message.ZuliaIndex.IndexShardMapping;

/**
 * A defined index this node knows about, resident or not.
 */
public record RegisteredIndex(String indexName, IndexShardMapping shardMapping) {

}

