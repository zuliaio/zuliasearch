package io.zulia.server.index.resident;

import io.zulia.server.index.ZuliaIndex;

@FunctionalInterface
public interface IndexLoader {

	/**
	 * Builds the index, loads its shards, applies any shard mapping transitions this node has not yet
	 * applied, and starts maintenance. Must not touch the cache except reads of the registry. Returns a
	 * started index or throws, never null: the cache publishes the result into a handle unconditionally.
	 */
	ZuliaIndex load(String indexName) throws Exception;
}