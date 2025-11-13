package io.zulia.server.serde;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.zulia.message.ZuliaBase;

public interface NodeStatsModifier {

	@JsonIgnore
	java.util.List<? extends io.zulia.message.ZuliaBase.IndexStatsOrBuilder> getIndexStatOrBuilderList();

	@JsonIgnore
	int getIndexStatCount();

	@JsonProperty("indexStat")
	java.util.List<? extends ZuliaBase.IndexStats> getIndexStatList();

	@JsonIgnore
	com.google.protobuf.ByteString getZuliaVersionBytes();

}
