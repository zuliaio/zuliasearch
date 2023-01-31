package io.zulia.server.exceptions;

import io.zulia.message.ZuliaBase.MasterSlaveSettings;

import java.io.IOException;

public class ShardOfflineException extends IOException {

	private static final long serialVersionUID = 1L;
	private final int shardNumber;
	private final MasterSlaveSettings masterSlaveSettings;
	private String indexName;

	public ShardOfflineException(String indexName, int shardNumber, MasterSlaveSettings masterSlaveSettings) {
		super("Index <" + indexName + "> with shardNumber <" + shardNumber + "> is offline for <" + masterSlaveSettings + ">");
		this.indexName = indexName;
		this.shardNumber = shardNumber;
		this.masterSlaveSettings = masterSlaveSettings;
	}

	public String getIndexName() {
		return indexName;
	}

	public int getShardNumber() {
		return shardNumber;
	}

	public MasterSlaveSettings getMasterSlaveSettings() {
		return masterSlaveSettings;
	}
}