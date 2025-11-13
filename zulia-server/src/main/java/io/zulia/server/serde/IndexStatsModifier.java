package io.zulia.server.serde;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public interface IndexStatsModifier {

	@JsonIgnore
	java.util.List<? extends io.zulia.message.ZuliaBase.ShardCacheStatsOrBuilder> getShardCacheStatOrBuilderList();


	@JsonProperty("shardCacheStat")
	java.util.List<? extends io.zulia.message.ZuliaBase.ShardCacheStats> getShardCacheStatList();

	@JsonIgnore
	int getShardCacheStatCount();

	@JsonIgnore
	com.google.protobuf.ByteString getIndexNameBytes();

}
