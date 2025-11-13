package io.zulia.server.serde;

import com.fasterxml.jackson.annotation.JsonIgnore;

public interface ShardCacheStatsModifier {

	@JsonIgnore
	io.zulia.message.ZuliaBase.CacheStatsOrBuilder getGeneralCacheOrBuilder();

	@JsonIgnore
	io.zulia.message.ZuliaBase.CacheStatsOrBuilder getPinnedCacheOrBuilder();
}
