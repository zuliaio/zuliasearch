package io.zulia.server.search.distinct;

import io.zulia.message.ZuliaQuery.DistinctCountInternal;
import io.zulia.message.ZuliaQuery.IndexShardResponse;
import io.zulia.message.ZuliaQuery.ShardQueryResponse;
import io.zulia.message.ZuliaServiceOuterClass.InternalQueryResponse;
import io.zulia.server.connection.client.InternalRpcConnection;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Bounds the distinct keys one node sends back in its internal query response, which carries every shard it answered for in one message.
 * The check runs on the keys really collected, so a field with few values passes however many shards a node holds.
 */
public final class DistinctKeyBudget {

	/**
	 * Distinct keys one node may send in one internal message, leaving 32 MB of the message limit for everything else in the response.
	 */
	public static final long MAX_KEY_BYTES_PER_MESSAGE = InternalRpcConnection.MAX_INBOUND_MESSAGE_BYTES - 32L * 1024 * 1024;

	private DistinctKeyBudget() {
	}

	/**
	 * Throws when the response's distinct keys would overflow the internal message, naming the fields, the node and the shard count.
	 */
	public static void check(InternalQueryResponse response, String node) {
		long keyBytes = 0;
		int shards = 0;
		Set<String> fields = new LinkedHashSet<>();
		for (IndexShardResponse indexShardResponse : response.getIndexShardResponseList()) {
			for (ShardQueryResponse shardQueryResponse : indexShardResponse.getShardQueryResponseList()) {
				shards++;
				for (DistinctCountInternal distinctCount : shardQueryResponse.getDistinctCountList()) {
					fields.add(distinctCount.getField());
					keyBytes += (long) distinctCount.getKeyCount() * Long.BYTES;
				}
			}
		}
		check(keyBytes, fields, shards, node);
	}

	static void check(long keyBytes, Set<String> fields, int shards, String node) {
		if (keyBytes > MAX_KEY_BYTES_PER_MESSAGE) {
			throw new IllegalArgumentException(
					"Distinct count keys for " + fields + " across " + shards + " shards on " + node + " total " + (keyBytes / 1_000_000)
							+ " MB, more than the " + (MAX_KEY_BYTES_PER_MESSAGE / 1_000_000)
							+ " MB one internal message may carry. Lower maxGroupsPerShard, count fewer fields at once, or spread the shards over more nodes");
		}
	}
}
